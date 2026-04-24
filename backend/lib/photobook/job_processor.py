import logging
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    AssetsDAL,
    PagesAssetsRelCreate,
    PagesAssetsRelDAL,
    PagesCreate,
    PagesDAL,
    PhotobooksDAL,
    PhotobooksUpdate,
)
from backend.db.dal.base import safe_commit
from backend.db.data_models import Jobs
from backend.lib.asset_manager.base import AssetManager
from backend.lib.types.asset import Asset
from backend.lib.utils.common import none_throws
from backend.lib.vertex_ai.gemini import Gemini


class JobProcessor:
    def __init__(
        self,
        job: Jobs,
        db_session: AsyncSession,
        asset_manager: AssetManager,
    ) -> None:
        self.job = job
        self.asset_manager = asset_manager
        self.db_session = db_session
        self.gemini = Gemini()

    async def process(self) -> dict[str, Any]:
        payload = none_throws(self.job.input_payload)
        asset_uuids = payload.get("asset_uuids", [])
        asset_objs = await AssetsDAL.get_by_ids(self.db_session, asset_uuids)
        orig_image_keys = [obj.asset_key_original for obj in asset_objs]

        originating_photobook = none_throws(
            await PhotobooksDAL.get_by_id(
                self.db_session, none_throws(self.job.photobook_id)
            )
        )
        logging.info(
            f"[job-processor] Processing job {self.job.id} created from photobook {originating_photobook.id}"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download
            tmp_path = Path(tmpdir)
            download_results = await self.asset_manager.download_files_batched(
                [(key, tmp_path / Path(key).name) for key in orig_image_keys]
            )
            downloaded_paths = [
                none_throws(asset.cached_local_path)
                for asset in download_results.values()
                if isinstance(asset, Asset)
            ]
            if not downloaded_paths:
                raise RuntimeError("All image downloads failed")
            img_filename_assets_map = {
                Path(asset.asset_key_original).name: asset for asset in asset_objs
            }

            # Run gemini
            try:
                # FIXME: retry strategy
                gemini_output = await self.gemini.run_image_understanding_job(
                    downloaded_paths,
                    originating_photobook.user_provided_occasion,
                    originating_photobook.user_provided_occasion_custom_details,
                    originating_photobook.user_provided_context,
                )
            except Exception as e:
                raise e

            # Persist photobook updates to DB
            async with safe_commit(self.db_session):
                page_create_objs = [
                    PagesCreate(
                        photobook_id=originating_photobook.id,
                        page_number=idx,
                        layout=None,
                        user_message=page_schema.page_message,
                    )
                    for idx, page_schema in enumerate(gemini_output.photobook_pages)
                ]
                pages = await PagesDAL.create_many(self.db_session, page_create_objs)

                pages_assets_rel_creates: list[PagesAssetsRelCreate] = []
                for page_schema, page in zip(gemini_output.photobook_pages, pages):
                    for idx, page_photo in enumerate(page_schema.page_photos):
                        asset_nullable = img_filename_assets_map.get(page_photo, None)
                        if asset_nullable is not None:
                            pages_assets_rel_creates.append(
                                PagesAssetsRelCreate(
                                    page_id=page.id,
                                    asset_id=asset_nullable.id,
                                    order_index=idx,
                                    caption=None,
                                )
                            )

                await PagesAssetsRelDAL.create_many(
                    self.db_session, pages_assets_rel_creates
                )
                await PhotobooksDAL.update_by_id(
                    self.db_session,
                    originating_photobook.id,
                    PhotobooksUpdate(
                        status="draft", title=gemini_output.photobook_title
                    ),
                )

        return {
            "job_id": str(self.job.id),
            "processed_keys": orig_image_keys,
            "successful_files": [str(p) for p in downloaded_paths],
            "gemini_raw_result": gemini_output.model_dump_json(),
        }

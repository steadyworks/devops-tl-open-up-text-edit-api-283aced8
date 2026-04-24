import logging
import uuid
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from backend.db.dal import (
    AssetsCreate,
    AssetsDAL,
    FilterOp,
    OrderDirection,
    PagesAssetsRelDAL,
    PagesDAL,
    PhotobooksCreate,
    PhotobooksDAL,
)
from backend.db.dal.base import safe_commit
from backend.db.data_models import UserProvidedOccasion
from backend.db.externals import (
    AssetsPublicModel,
    PagesPublicModel,
    PhotobooksPublicModel,
)
from backend.lib.job_manager.base import JobType
from backend.lib.types.asset import Asset
from backend.lib.utils.common import none_throws
from backend.lib.utils.web_requests import UploadFileTempDirManager
from backend.route_handler.base import RouteHandler


class UploadedFileInfo(BaseModel):
    filename: str
    storage_key: str


class FailedUploadInfo(BaseModel):
    filename: str
    error: str


class NewPhotobookResponse(BaseModel):
    job_id: UUID
    photobook_id: UUID
    uploaded_files: list[UploadedFileInfo]
    failed_uploads: list[FailedUploadInfo]
    skipped_non_media: list[str]


class AssetResponse(AssetsPublicModel):
    asset_key_original: str = Field(exclude=True)
    asset_key_display: Optional[str] = Field(exclude=True)
    asset_key_llm: Optional[str] = Field(exclude=True)
    signed_asset_url: str


class PageResponse(PagesPublicModel):
    assets: list[AssetResponse]


class PhotobookResponse(PhotobooksPublicModel):
    pages: list[PageResponse]


class TimelensAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.router.add_api_route(
            "/api/photobook/new",
            self.new_photobook,
            methods=["POST"],
            response_model=NewPhotobookResponse,
        )
        self.router.add_api_route(
            "/api/photobook/{photobook_id}",
            self.get_photobook_by_id,
            methods=["GET"],
            response_model=PhotobookResponse,
        )

    @staticmethod
    def is_accepted_mime(mime: Optional[str]) -> bool:
        return mime is not None and (
            mime.startswith("image/")
            # or mime.startswith("video/") # only images allowed for now
        )

    async def new_photobook(
        self,
        files: list[UploadFile] = File(...),
        user_provided_occasion: UserProvidedOccasion = Form(...),
        user_provided_custom_details: Optional[str] = Form(None),
        user_provided_context: Optional[str] = Form(None),
    ) -> NewPhotobookResponse:
        async with self.app.db_session_factory.session() as db_session:
            # Filter valid files according to FastAPI reported mime type
            valid_files = [
                file
                for file in files
                if TimelensAPIHandler.is_accepted_mime(file.content_type)
            ]
            file_names = [file.filename for file in valid_files]
            skipped = [
                file.filename
                for file in files
                if file not in valid_files and file.filename is not None
            ]
            logging.info({"accepted_files": file_names, "skipped_non_media": skipped})

            succeeded_uploads: list[UploadedFileInfo] = []
            failed_uploads: list[FailedUploadInfo] = []

            USER_ID_FIXME = uuid.uuid4()

            async with UploadFileTempDirManager(
                str(uuid.uuid4()),
                valid_files,  # FIXME
            ) as user_requested_uploads:
                # 1. Create photobook in DB
                async with safe_commit(db_session):
                    photobook = await PhotobooksDAL.create(
                        db_session,
                        PhotobooksCreate(
                            user_id=USER_ID_FIXME,  # FIXME: hardcoded
                            title=f"New Photobook {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                            caption=None,
                            theme=None,
                            status="pending",
                            user_provided_occasion=user_provided_occasion,
                            user_provided_occasion_custom_details=user_provided_custom_details,
                            user_provided_context=user_provided_context,
                        ),
                    )
                    await db_session.commit()

                upload_inputs = [
                    (
                        none_throws(asset.cached_local_path),
                        self.app.asset_manager.mint_asset_key(
                            photobook.id, none_throws(asset.cached_local_path).name
                        ),
                    )
                    for (_original_fname, asset) in user_requested_uploads
                ]
                upload_results = await self.app.asset_manager.upload_files_batched(
                    upload_inputs
                )
                asset_objs_to_create: list[AssetsCreate] = []

                # 2. Transform upload results into endpoint response
                for _original_fname, asset in user_requested_uploads:
                    upload_res = upload_results.get(
                        none_throws(asset.cached_local_path), None
                    )
                    if upload_res is None or isinstance(upload_res, Exception):
                        failed_uploads.append(
                            FailedUploadInfo(
                                filename=_original_fname, error=str(upload_res)
                            )
                        )
                    else:
                        assert isinstance(upload_res, Asset)
                        succeeded_uploads.append(
                            UploadedFileInfo(
                                filename=_original_fname,
                                storage_key=none_throws(upload_res.asset_storage_key),
                            )
                        )
                        asset_objs_to_create.append(
                            AssetsCreate(
                                user_id=USER_ID_FIXME,
                                asset_key_original=none_throws(
                                    upload_res.asset_storage_key
                                ),
                                asset_key_display=None,
                                asset_key_llm=None,
                                metadata_json={},
                                original_photobook_id=photobook.id,
                            )
                        )

            # 3. Batch-insert assets
            async with safe_commit(db_session):
                created_assets = await AssetsDAL.create_many(
                    db_session, asset_objs_to_create
                )
                await db_session.commit()

            # 4. Enqueue photobook generation job
            job_id = await self.app.job_manager.enqueue(
                db_session,
                JobType.PHOTOBOOK_GENERATION,
                USER_ID_FIXME,
                photobook,
                {
                    "asset_uuids": [str(asset.id) for asset in created_assets],
                },
            )

            return NewPhotobookResponse(
                job_id=job_id,
                photobook_id=photobook.id,
                uploaded_files=succeeded_uploads,
                failed_uploads=failed_uploads,
                skipped_non_media=skipped,
            )

    async def get_photobook_by_id(
        self,
        photobook_id: UUID,
    ) -> PhotobookResponse:
        async with self.app.db_session_factory.session() as db_session:
            # Step 1: Fetch photobook
            photobook = await PhotobooksDAL.get_by_id(db_session, photobook_id)
            if photobook is None:
                raise HTTPException(status_code=404, detail="Photobook not found")

            # Step 2: Fetch pages for the photobook
            pages = await PagesDAL.list_all(
                db_session,
                filters={"photobook_id": (FilterOp.EQ, photobook_id)},
                order_by=[("page_number", OrderDirection.ASC)],
            )

            # Step 3: Fetch all page→asset relationships
            page_ids = [page.id for page in pages]
            page_asset_rels = await PagesAssetsRelDAL.list_all(
                db_session,
                filters={"page_id": (FilterOp.IN, page_ids)},
                order_by=[("order_index", OrderDirection.ASC)],
            )

            # Step 4: Collect all asset_ids used
            asset_ids = [rel.asset_id for rel in page_asset_rels if rel.asset_id]
            asset_list = await AssetsDAL.get_by_ids(db_session, asset_ids)
            assets_by_id = {asset.id: asset for asset in asset_list}

            # Step 5: Generate signed URLs for original asset keys
            asset_keys = [
                asset.asset_key_original
                for asset in asset_list
                if asset.asset_key_original
            ]
            signed_urls = await self.app.asset_manager.generate_signed_urls_batched(
                asset_keys
            )

            # Step 6: Assemble response
            page_id_to_assets: dict[UUID, list[AssetResponse]] = {}
            for rel in page_asset_rels:
                if rel.page_id and rel.asset_id:
                    asset = assets_by_id[rel.asset_id]
                    signed_url = signed_urls.get(asset.asset_key_original)
                    # Inject signed URL into the model
                    asset_with_url = AssetResponse(
                        **AssetsPublicModel.model_validate(asset).model_dump(),
                        signed_asset_url=(
                            signed_url if isinstance(signed_url, str) else ""
                        ),
                    )

                    page_id_to_assets.setdefault(rel.page_id, []).append(asset_with_url)

        return PhotobookResponse(
            **PhotobooksPublicModel.model_validate(photobook).model_dump(),
            pages=[
                PageResponse(
                    **PagesPublicModel.model_validate(page).model_dump(),
                    assets=page_id_to_assets.get(page.id, []),
                )
                for page in pages
            ],
        )

# job_manager.py
import datetime
import os
from enum import Enum
from typing import Any, Literal, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import JobsCreate, JobsDAL, JobsUpdate
from backend.db.dal.base import safe_commit
from backend.db.data_models import Jobs, Photobooks
from backend.lib.redis.client import RedisClient


class JobQueue(Enum):
    MAIN_TASK_QUEUE = "main_task_queue"


class JobType(str, Enum):
    PHOTOBOOK_GENERATION = "photobook_generation"


class JobManager:
    @classmethod
    def __get_queue_name(cls, queue: JobQueue) -> str:
        prefix = "PROD_" if os.getenv("ENV") == "production" else "DEV_"
        return prefix + str(queue)

    def __init__(self, redis_client: RedisClient, queue: JobQueue) -> None:
        self.redis = redis_client
        self.queue_name = JobManager.__get_queue_name(queue)

    async def enqueue(
        self,
        db_session: AsyncSession,
        job_type: JobType,
        user_id: UUID,  # FIXME
        photobook: Photobooks,
        job_payload: dict[str, Any],
    ) -> UUID:
        # Step 1: Persist job in Postgres
        async with safe_commit(db_session):
            job = await JobsDAL.create(
                db_session,
                JobsCreate(
                    job_type=job_type,
                    status="queued",
                    user_id=user_id,
                    photobook_id=photobook.id,
                    input_payload=job_payload,
                    result_payload=None,
                    error_message=None,
                    started_at=None,
                    completed_at=None,
                ),
            )
            await db_session.commit()

        # Step 2: Enqueue job ID in Redis
        await self.redis.client.rpush(self.queue_name, str(job.id))

        return job.id

    async def dequeue(
        self,
        db_session: AsyncSession,
        timeout: Optional[int] = 0,
    ) -> Optional[Jobs]:
        result = await self.redis.client.blpop(self.queue_name, timeout=timeout)
        if not result:
            return None  # timeout occurred

        async with safe_commit(db_session):
            _job_queue_name, job_id_str = result
            try:
                job_id = UUID(job_id_str)
            except ValueError:
                # Optionally log and skip
                return None

            # Update job status in Postgres
            updated_job = await JobsDAL.update_by_id(
                db_session,
                job_id,
                JobsUpdate(
                    status="dequeued",
                    started_at=datetime.datetime.now(datetime.timezone.utc),
                ),
            )
            await db_session.commit()
            await db_session.refresh(updated_job)

        return updated_job

    async def update_status(
        self,
        db_session: AsyncSession,
        job_id: UUID,
        status: Literal["processing", "done", "error"],
        error_message: Optional[str] = None,
        result_payload: Optional[dict[str, Any]] = None,
    ) -> None:
        async with safe_commit(db_session):
            update_data = JobsUpdate(
                status=status,
                error_message=error_message,
                result_payload=result_payload,
                completed_at=datetime.datetime.now(datetime.timezone.utc)
                if status == "done"
                else None,
            )
            await JobsDAL.update_by_id(db_session, job_id, update_data)
            await db_session.commit()

    async def get_status(
        self,
        db_session: AsyncSession,
        job_id: UUID,
    ) -> Jobs:
        job = await JobsDAL.get_by_id(db_session, job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        return job

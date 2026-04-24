import asyncio
import logging
import multiprocessing as mp
import os
import threading
import time
import traceback
from multiprocessing.connection import Connection

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import Jobs
from backend.db.session.factory import AsyncSessionFactory
from backend.lib.asset_manager.base import AssetManager
from backend.lib.asset_manager.factory import AssetManagerFactory
from backend.lib.job_manager.base import JobManager, JobQueue
from backend.lib.photobook.job_processor import JobProcessor
from backend.lib.redis.client import RedisClient
from backend.logging_utils import configure_logging_env
from backend.path_manager import PathManager

configure_logging_env()

# Load environment-specific file
env_file = ".env.prod" if os.getenv("ENV") == "production" else ".env.dev"
loaded = load_dotenv(dotenv_path=PathManager().get_repo_root() / env_file)
assert loaded, "Env not loaded"

MAX_JOB_TIMEOUT_SECS = 600  # 10 mins
SEND_HEARTBEAT_EVERY_SECS = 1
POLL_SHUTDOWN_EVERY_SECS = 1


class WorkerProcess(mp.Process):
    def __init__(self, heartbeat_connection: Connection, name: str = "worker"):
        super().__init__()
        self.name = name
        self.heartbeat_connection = heartbeat_connection

    def run(self) -> None:
        try:
            redis = RedisClient()
            job_manager = JobManager(redis, JobQueue.MAIN_TASK_QUEUE)
            asset_manager = AssetManagerFactory().create()
            session_factory = AsyncSessionFactory()

            def send_heartbeat() -> None:
                while True:
                    try:
                        self.heartbeat_connection.send("ping")
                        time.sleep(SEND_HEARTBEAT_EVERY_SECS)
                    except Exception:
                        break  # parent closed pipe

            threading.Thread(target=send_heartbeat, daemon=True).start()

            asyncio.run(self._main_loop(job_manager, asset_manager, session_factory))
        except Exception as e:
            logging.exception(f"[{self.name}] Worker crashed: {e}")

    async def _main_loop(
        self,
        job_manager: JobManager,
        asset_manager: AssetManager,
        session_factory: AsyncSessionFactory,
    ) -> None:
        logging.info(f"[{self.name}] Started worker process (PID={self.pid})")
        while True:
            # 1. Check for shutdown message
            if self.heartbeat_connection.poll(timeout=POLL_SHUTDOWN_EVERY_SECS):
                try:
                    msg = self.heartbeat_connection.recv()
                    if msg == "shutdown":
                        logging.info(f"[{self.name}] Received shutdown signal")
                        break
                except EOFError:
                    logging.warning(f"[{self.name}] Heartbeat pipe closed")
                    break

            try:
                async with session_factory.session() as db_session:
                    job = await job_manager.dequeue(db_session, timeout=5)
                    if not job:
                        continue  # No job this cycle

                    try:
                        await asyncio.wait_for(
                            self._handle_task(
                                job, job_manager, asset_manager, db_session
                            ),
                            timeout=MAX_JOB_TIMEOUT_SECS,
                        )
                    except asyncio.TimeoutError:
                        logging.warning(
                            f"[{self.name}] Job timed out after {MAX_JOB_TIMEOUT_SECS}s, "
                            f"job_id: {job.id} payload: {job.input_payload}"
                        )
                        await job_manager.update_status(
                            db_session,
                            job.id,
                            "error",
                            error_message=f"Timeout after {MAX_JOB_TIMEOUT_SECS}s",
                        )
            except Exception as e:
                logging.exception(f"[{self.name}] Unexpected worker error: {e}")

        logging.info(f"[{self.name}] Exiting cleanly")

    async def _handle_task(
        self,
        job: Jobs,
        job_manager: JobManager,
        asset_manager: AssetManager,
        db_session: AsyncSession,
    ) -> None:
        try:
            await job_manager.update_status(db_session, job.id, "processing")
            processor = JobProcessor(job, db_session, asset_manager)
            try:
                result = await processor.process()
            except Exception as e:
                logging.exception(
                    f"[{self.name}] Processor failed for job {job.id}: {e}"
                )
                await job_manager.update_status(
                    db_session, job.id, "error", error_message=str(e)
                )
                return

            await job_manager.update_status(
                db_session, job.id, "done", result_payload=result
            )
            logging.info(f"[{self.name}] Job {job.id} completed with result: {result}")

        except Exception as e:
            traceback.print_exc()
            logging.warning(f"[{self.name}] Failed job {job.id}: {e}")
            await job_manager.update_status(
                db_session, job.id, "error", error_message=str(e)
            )

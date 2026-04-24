from uuid import UUID

from fastapi.responses import JSONResponse

from backend.route_handler.base import RouteHandler


class DebugHandler(RouteHandler):
    def register_routes(self) -> None:
        self.router.add_api_route("/api/debug", self.debug, methods=["GET"])
        self.router.add_api_route(
            "/api/debug/sentry-debug",
            self.sentry_debug,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/debug/test-get-job-status/{job_id}",
            self.test_get_job_status,
            methods=["GET"],
        )

    async def debug(self) -> JSONResponse:
        return JSONResponse({"hello": "world"})

    async def sentry_debug(self) -> JSONResponse:
        _division_by_zero = 1 / 0
        return JSONResponse("")

    async def test_get_job_status(self, job_id: UUID) -> JSONResponse:
        async with self.app.db_session_factory.session() as db_session:
            job = await self.app.job_manager.get_status(db_session, job_id)
            return JSONResponse(
                {
                    "status": job.status,
                    "error": job.error_message,
                    "result": job.result_payload,
                    "job_id": str(job.id),
                }
            )

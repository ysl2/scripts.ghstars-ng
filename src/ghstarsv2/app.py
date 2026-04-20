from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse

from src.ghstarsv2.api import register_routes
from src.ghstarsv2.config import get_settings
from src.ghstarsv2.jobs import init_database


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_database()
    yield


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        if len(errors) == 1:
            message = str(errors[0].get("msg") or "Invalid request")
            return JSONResponse(
                status_code=422,
                content={"detail": message.removeprefix("Value error, ")},
            )
        return JSONResponse(status_code=422, content={"detail": errors})


def _register_frontend(app: FastAPI) -> None:
    dist_dir = get_settings().frontend_dist_dir.resolve()
    index_file = dist_dir / "index.html"
    if not index_file.exists():
        return

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_frontend(full_path: str) -> FileResponse:
        api_prefix = get_settings().api_prefix.lstrip("/")
        if full_path.startswith(api_prefix):
            raise HTTPException(status_code=404)

        requested = (dist_dir / full_path).resolve()
        if dist_dir == requested or dist_dir in requested.parents:
            if requested.is_file():
                return FileResponse(requested)
        return FileResponse(index_file)


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _register_exception_handlers(app)
    register_routes(app)
    _register_frontend(app)
    return app


app = create_app()

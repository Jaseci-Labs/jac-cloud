"""JacLang FastAPI Core."""

from os import getenv
from typing import Any

from fastapi import FastAPI as _FaststAPI

from uvicorn import run as _run

HOST = getenv("HOST", "0.0.0.0")
PORT = int(getenv("HOST", "8000"))


class FastAPI:
    """FastAPI Handler."""

    __app__ = None

    @classmethod
    def get(cls) -> _FaststAPI:
        """Get or Create new instance of FastAPI."""
        if not isinstance(cls.__app__, _FaststAPI):
            cls.__app__ = _FaststAPI()

            from ..routers import healthz_router, user_router
            from ..plugins import walker_router

            for router in [healthz_router, user_router, walker_router]:
                cls.__app__.include_router(router)

        return cls.__app__

    @classmethod
    def start(
        cls, host: str = HOST, port: int = PORT, **kwargs: Any  # noqa ANN401
    ) -> None:
        """Run FastAPI Handler via Uvicorn."""
        _run(cls.get(), host=host, port=port, **kwargs)

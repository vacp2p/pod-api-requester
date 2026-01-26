from fastapi import FastAPI

from routers.generic import create_router as create_generic_router
from utils import setup_logger

logger = setup_logger(__file__)

from fastapi import FastAPI

from utils import setup_logger

logger = setup_logger(__file__)


def create_app(config) -> FastAPI:
    app = FastAPI()

    app.state.namespace = (
        open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip() or "default"
    )

    async def get_config() -> dict:
        logger.debug(f"get_config: {config}")
        return config

    app.include_router(create_generic_router(get_config))

    return app

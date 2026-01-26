from fastapi import FastAPI

from routers.registry import build_routers
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

    for router in build_routers(get_config):
        app.include_router(router)

    return app

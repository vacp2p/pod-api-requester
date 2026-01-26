from typing import Awaitable, Callable

from fastapi import APIRouter, Depends, Request

from common import call_endpoint, get_pod_infos
from configs import ConfigRequest
from routers.deps import (
    InvokeRequestData,
    endpoint_error_handler,
    unwrap_arg,
)
from schemas import NotFoundError
from utils import setup_logger

logger = setup_logger(__file__)


def create_router(get_config: Callable[[], Awaitable[dict]]) -> APIRouter:
    router = APIRouter()

    @router.post("/process")
    @endpoint_error_handler
    async def process_data(
        request: Request,
        data: InvokeRequestData,
        config=Depends(get_config),
    ):
        target = unwrap_arg(data.target, "targets", config)
        endpoint = unwrap_arg(data.endpoint, "endpoints", config)

        configRequest = ConfigRequest(
            name="dummy_request",
            endpoint=endpoint,
            retries=0,
            retry_delay=0,
        )
        try:
            pod_info = next(iter(get_pod_infos([target], namespace=request.app.state.namespace)))
        except StopIteration as e:
            raise NotFoundError(f"Target not found. Target: {target}") from e

        result = call_endpoint(configRequest.endpoint, pod_info)
        return result

    return router

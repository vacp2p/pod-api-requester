from typing import Annotated, Awaitable, Callable, Optional, Union

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, PositiveFloat, PositiveInt

from async_client import run_load_test
from common import call_endpoint, get_pod_infos
from configs import ConfigAction, ConfigRequest, LoadTestConfig
from routers.deps import InvokeRequestData, TargetConfig, TargetName, endpoint_error_handler, unwrap_arg
from schemas import NotFoundError
from utils import setup_logger

logger = setup_logger(__file__)


class LoadTestInlineRequest(BaseModel):
    """Inline load test request."""
    target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
    endpoint: str

    rate_per_pod: Optional[PositiveFloat] = None
    messages_per_pod: Optional[PositiveInt] = None
    duration_seconds: Optional[PositiveFloat] = None
    burst_size: Optional[PositiveInt] = None
    burst_delay: Optional[PositiveFloat] = None


def create_router(get_config: Callable[[], Awaitable[dict]]) -> APIRouter:
    router = APIRouter()

    @router.post("/process")
    @endpoint_error_handler
    async def process_data(
        request: Request,
        data: InvokeRequestData,
        config=Depends(get_config),
    ):
        logger.info(f"/process. request: `{request}` data: `{data}`")
        target = unwrap_arg(data.target, "targets", config)
        endpoint = unwrap_arg(data.endpoint, "endpoints", config)

        configRequest = ConfigRequest(
            name="dummy_request",
            endpoint=endpoint,
            retries=0,
            retry_delay=0,
        )
        try:
            pods = get_pod_infos(
                targets=[target],
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"Target not found. Target: {target}") from e

        result = call_endpoint(configRequest.endpoint, pod_info)
        return result

    @router.post("/cache/clear")
    @endpoint_error_handler
    async def process_data(
        request: Request,
        config=Depends(get_config),
    ):
        request.app.state.cache.clear()
        return {"cleared": True}

    @router.get("/loadtest/actions")
    @endpoint_error_handler
    async def list_loadtest_actions(config=Depends(get_config)):
        """List actions with load_test enabled."""
        return {
            "actions": list(config["actions"].keys()),
            "load_test_actions": [
                name for name, action in config["actions"].items()
                if action.load_test.enabled
            ],
        }

    @router.post("/loadtest/run/{action_name}")
    @endpoint_error_handler
    async def run_loadtest_action(
        request: Request,
        action_name: str,
        config=Depends(get_config),
    ):
        """Run a load test action defined in config.yaml. 
        Targets the first pod in the list to act as the message injector.
        """
        if action_name not in config["actions"]:
            raise NotFoundError(f"Action not found: {action_name}")

        action: ConfigAction = config["actions"][action_name]

        if not action.load_test.enabled:
            return {"error": "load_test not enabled for this action"}

        try:
            pods = get_pod_infos(
                targets=action.targets,
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"No pods found for action: {action_name}") from e

        logger.info(f"load_test action={action_name} pod={pod_info.pod_name}")
        return await run_load_test(action, [pod_info])

    @router.post("/loadtest/inline")
    @endpoint_error_handler
    async def run_inline_loadtest(
        request: Request,
        data: LoadTestInlineRequest,
        config=Depends(get_config),
    ):
        """Run an ad-hoc load test with parameters from the request body.
        Targets the first pod in the list to act as the message injector.
        """
        target = unwrap_arg(data.target, "targets", config)

        if data.endpoint not in config["endpoints"]:
            raise NotFoundError(f"Endpoint not found: {data.endpoint}")

        endpoint = config["endpoints"][data.endpoint]

        load_test_config = LoadTestConfig(
            enabled=True,
            rate_per_pod=data.rate_per_pod,
            messages_per_pod=data.messages_per_pod,
            duration_seconds=data.duration_seconds,
            burst_size=data.burst_size,
            burst_delay=data.burst_delay,
        )
        load_test_config.validate_config()

        inline_action = ConfigAction(
            name="inline_load_test",
            loop_order="foreach_pod_make_all_requests",
            targets=[target],
            requests=[ConfigRequest(name="inline", endpoint=endpoint, retries=0, retry_delay=0)],
            order="ascending",
            load_test=load_test_config,
        )

        try:
            pods = get_pod_infos(
                targets=[target],
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"No pods found for target: {target.name}") from e

        logger.info(f"inline load_test pod={pod_info.pod_name} endpoint={data.endpoint}")
        return await run_load_test(inline_action, [pod_info])

    return router

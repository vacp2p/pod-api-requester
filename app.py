from functools import wraps
import traceback
from typing import Annotated, Literal, Union

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from common import call_endpoint, get_pod_infos
from configs import ConfigEndpoint, ConfigRequest, ConfigTarget
from schemas import NotFoundError
from utils import setup_logger

logger = setup_logger(__file__)

def endpoint_error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPException:
            # Let already-constructed HTTPExceptions pass through untouched
            raise
        except Exception as e:
            message = f"{e!r}\n{traceback.format_exc()}"
            logger.error(message)
            raise HTTPException(status_code=500, detail=message) from e
    return wrapper

def create_app(config) -> FastAPI:
    app = FastAPI()

    app.state.namespace = (
        open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read() or "default"
    )

    async def get_config() -> dict:
        logger.debug(f"get_config: {config}")
        return config

    class TargetConfig(BaseModel):
        kind: Literal["config"]
        value: ConfigTarget

    class TargetName(BaseModel):
        kind: Literal["name"]
        value: str

    class EndpointConfig(BaseModel):
        kind: Literal["config"]
        value: ConfigEndpoint

    class EndpointName(BaseModel):
        kind: Literal["name"]
        value: str

    class InvokeRequestData(BaseModel):
        target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
        endpoint: Annotated[Union[EndpointName, EndpointConfig], Field(discriminator="kind")]

    @app.post("/process")
    @endpoint_error_handler()
    def process_data(data: InvokeRequestData, config=Depends(get_config)):
        """
        Performs an API request to the given endpoint on the given target.

        :param data: Contains target and endpoint.
            For each, the argument may either the name from the config,
            or a custom object passed in as a dict.

        Sample usage (from outside the cluster):
            data = {
                "target": {
                    "name": "dummy",
                    "service": "zerotesting-lightpush-client",
                    "name_template": "lpclient-0-0",
                },
                "endpoint": "lightpush-publish-static-sharding",
            }
            url = f"http://{external_ip}:{node_port}/process"
            response = requests.post(url, json=data)
        """

        def unwrap_arg(arg, key):
            if isinstance(arg.value, str):
                try:
                    # Treat as the name of a from config.
                    return config[key][arg.value]
                except KeyError as e:
                    raise NotFoundError(
                        f"Config object not found. Key: `{key}` Target: `{arg}`"
                    ) from e
            else:
                # Treat as custom config.
                return arg.value

        try:
            target = unwrap_arg(data.target, "targets")
            endpoint = unwrap_arg(data.endpoint, "endpoints")

            request = ConfigRequest(
                name="dummy_request", endpoint=endpoint, retries=0, retry_delay=0
            )
            try:
                pod_info = next(iter(get_pod_infos([target], namespace=app.state.namespace)))
            except StopIteration as e:
                raise NotFoundError(f"Target not found. Target: {target}") from e
            result = call_endpoint(request.endpoint, pod_info)
            return result
        except Exception as e:
            logger.error(HTTPException(status_code=500, detail=f"{e!r}\n{traceback.format_exc()}"))
            raise HTTPException(status_code=500, detail=f"{e!r}\n{traceback.format_exc()}") from e

    return app

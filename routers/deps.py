import traceback
from functools import wraps
from typing import Annotated, Callable, Literal, ParamSpec, TypeVar, Union

from fastapi import HTTPException
from pydantic import BaseModel, Field

from configs import ConfigEndpoint, ConfigTarget
from schemas import NotFoundError
from utils import setup_logger

logger = setup_logger(__file__)

P = ParamSpec("P")
R = TypeVar("R")


def unwrap_arg(arg, key, config):
    if isinstance(arg.value, str):
        try:
            # Treat as the name of a from config.
            return config[key][arg.value]
        except KeyError as e:
            raise NotFoundError(f"Config object not found. Key: `{key}` Target: `{arg}`") from e
    else:
        # Treat as custom config.
        return arg.value


def endpoint_error_handler(func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except HTTPException:
            raise
        except Exception as e:
            message = f"{e!r}\n{traceback.format_exc()}"
            logger.error(message)
            raise HTTPException(status_code=500, detail=message) from e

    return wrapper


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

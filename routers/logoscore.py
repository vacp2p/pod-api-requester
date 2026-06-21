"""Integration for logoscore interaction."""

import base64
import json
from pathlib import Path
from typing import Annotated, Awaitable, Callable, Optional, Union

from fastapi import APIRouter, Depends, Request
from kubernetes import client
from logoscore import LogoscoreClient
from pydantic import BaseModel, Field

from common import get_pod_infos
from kube_client import core_v1
from routers.deps import TargetConfig, TargetName, endpoint_error_handler, unwrap_arg
from schemas import NotFoundError, TargetPodInfo
from utils import setup_logger

logger = setup_logger(__file__)

CONFIG_DIR = Path("~/.logoscore/").expanduser()


def get_token_from_secret(
    secret_name: str,
    namespace: str,
    file_name: Optional[str] = None,
) -> dict:
    """
    :param secret_name: eg. "alicesecret"
    :param namespace: eg. "zerotesting"
    :param file_name: Key inside the secret json containing the token. eg. "alice.json". Leave None to use the first key.
    """
    HOST = "https://kubernetes.default.svc"
    CA_CERT = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    with open(TOKEN_FILE, "r") as f:
        token = f.read().strip()

    configuration = client.Configuration()
    configuration.host = HOST
    configuration.ssl_ca_cert = CA_CERT
    configuration.api_key["authorization"] = token
    configuration.api_key_prefix["authorization"] = "Bearer"

    api_client = client.ApiClient(configuration)
    api_client.default_headers["Authorization"] = f"Bearer {token}"
    auth_header = api_client.default_headers.get("Authorization")
    logger.debug(f"Client Header: {auth_header}")

    secret = core_v1.read_namespaced_secret(name=secret_name, namespace=namespace)

    name = file_name or next(iter(secret.data))

    full_token_str = base64.b64decode(secret.data[name]).strip().decode("utf-8")
    return json.loads(full_token_str)


def save_token(token: dict, config_dir: str, file_name: str):
    config_file = Path(config_dir) / "client" / file_name
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(token, f, indent=4)


def make_config(token_file_name: str, ip: str):
    config = {
        "version": 2,
        "token_file": token_file_name,
        "daemon": {
            "core_service": {"transport": "tcp", "host": ip, "port": 8645},
            "capability_module": {"transport": "tcp", "host": ip, "port": 8646},
        },
    }
    with open(CONFIG_DIR / "client" / "config.json", "w") as config_file:
        json.dump(config, config_file, indent=4)


def secret_for(target: str) -> str:
    return f"{target}-logoscore-secret"


def token_file_for(target) -> str:
    return f"{target}_token.json"


def init_token(target, namespace):
    result_data = {}
    try:
        secret = secret_for(target)
        token_file = token_file_for(target)
        token = get_token_from_secret(secret, namespace)
        save_token(token, CONFIG_DIR, token_file)

        result_data["response"] = {
            "status_code": 200,
            "text": json.dumps(
                {"config_dir": CONFIG_DIR.as_posix(), "token_file": token_file},
            ),
        }
    except Exception as e:
        result_data["exception"] = str(e)

    logger.debug(f"init_token result: {result_data}")
    return result_data


def make_call(target: TargetPodInfo, params: dict):
    namespace = target.pod.metadata.namespace
    pod_name = target.pod_name
    token_file = token_file_for(pod_name)
    target_host = f"{pod_name}.{namespace}.svc.cluster.local"
    target_ip = target.pod.status.pod_ip
    debug_dict = {"target": {"host": target_host, "ip": target_ip}, "params": params}
    logger.debug(f"make_call {json.dumps(debug_dict, indent=4)}")
    make_config(token_file, target_ip)

    result_data = {"request": debug_dict}

    lclient = LogoscoreClient(config_dir=CONFIG_DIR.as_posix())

    module = params["module"]
    function = params["function"]
    func_params = params["params"]

    if isinstance(func_params, dict):
        func_params = json.dumps(func_params)

    try:
        if func_params is None:
            result = lclient.call(module, function)
        elif isinstance(func_params, (list, tuple)):
            result = lclient.call(module, function, *func_params)
        else:
            result = lclient.call(module, function, func_params)

        result_data["response"] = {
            "status_code": 200 if result["success"] else 500,
            "text": json.dumps(result),
        }
    except Exception as e:
        result_data["exception"] = e

    logger.debug(f"make_call result: {result_data}")
    return result_data


class LogosCoreRequestData(BaseModel):
    target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
    params: Optional[dict] = None


def create_router(get_config: Callable[[], Awaitable[dict]]) -> APIRouter:
    router = APIRouter()

    @router.post("/logoscore/init")
    @endpoint_error_handler
    async def init(
        request: Request,
        data: LogosCoreRequestData,
        config=Depends(get_config),
    ):
        target = unwrap_arg(data.target, "targets", config)

        try:
            pods = get_pod_infos(
                targets=[target],
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"Target not found. Target: {target}") from e

        namespace = pod_info.pod.metadata.namespace
        return init_token(pod_info.pod_name, namespace)

    @router.post("/logoscore/call")
    @endpoint_error_handler
    async def call(
        request: Request,
        data: LogosCoreRequestData,
        config=Depends(get_config),
    ):
        target = unwrap_arg(data.target, "targets", config)
        params = data.params

        try:
            pods = get_pod_infos(
                targets=[target],
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"Target not found. Target: {target}") from e

        return make_call(pod_info, params)

    return router

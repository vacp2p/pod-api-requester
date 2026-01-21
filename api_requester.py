import argparse
import random
import traceback
from argparse import Namespace
from collections import defaultdict
from pathlib import Path
from typing import Annotated, Dict, List, Literal, Union

import requests
import uvicorn
import yaml
from fastapi import Depends, FastAPI, HTTPException
from kubernetes import client, config
from kubernetes.client.models.v1_pod import V1Pod
from pydantic import BaseModel, ConfigDict, Field

from configs import ConfigAction, ConfigEndpoint, ConfigRequest, ConfigTarget
from utils import paged_request, setup_logger


class TargetPodInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config_target: ConfigTarget
    pod: V1Pod

    @property
    def pod_name(self) -> str:
        return self.pod.metadata.name


logger = setup_logger(__file__)


app = FastAPI()


class NotFoundError(LookupError):
    """Raised when no object matches the given criteria.

    For example specifying a target name that does not exist in the ConfigMap."""


def do_request(request: ConfigRequest, pod_info: TargetPodInfo):
    raise NotImplementedError()


def call_endpoint(endpoint: ConfigEndpoint, pod_info: TargetPodInfo) -> dict:
    result_data = {"request": {"configEndpoint": endpoint}}
    request_data = {"params": endpoint.params, "headers": endpoint.headers}

    try:
        request_data["url"] = endpoint.url.format(
            node=pod_info.pod.status.pod_ip, port=pod_info.config_target.port
        )
        request_data["pod"] = f"{pod_info.pod.metadata.name}"
        logger.info(f"request_data: {request_data}")

        if endpoint.paged:
            if endpoint.type != "GET":
                raise NotImplementedError("Paged requests only implemented for GET requests.")
            result = paged_request(request=request_data, max_attempts=1, page_request_delay=0)
        else:
            if endpoint.type == "POST":
                result = requests.post(
                    request_data["url"],
                    json=request_data["params"],
                    headers=request_data["headers"],
                )
            elif endpoint.type == "GET":
                result = requests.post(
                    request_data["url"],
                    json=request_data["params"],
                    headers=request_data["headers"],
                )
            else:
                raise AttributeError(f"Unknown request type. request: `{endpoint}`")

        result_data["request"].update(request_data)
        result_data["response"] = {"status_code": result.status_code, "text": result.text}
    except Exception as e:
        error = traceback.format_exc()
        logger.error(
            f"Exception attempting API request. endpoint: `{endpoint}`, exception: `{e}`, error: `{error}`"
        )
        result_data["exception"] = error

    logger.info(result_data)
    return result_data


def assert_unique_attr(objects: List[object], attribute: str):
    names = [getattr(endpoint, attribute) for endpoint in objects]
    duplicates = set()
    seen = []

    for name in names:
        if any(name == item for item in seen):
            duplicates.add(name)
        else:
            seen.append(name)

    assert not duplicates, (
        f"At least one object has the same attribute as another. "
        f"Attribute name: `{attribute}`. "
        f"Duplicate attributes: `{duplicates}`. "
        f"Objects: `{objects}`"
    )


def get_pods_for_target(target: ConfigTarget) -> List[str]:
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read() or "default"

    if target.service is not None:
        pods = v1.list_namespaced_pod(namespace)
    else:
        service = v1.read_namespaced_service(target.service, namespace)
        selector = service.spec.selector
        selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
        pods = v1.list_namespaced_pod(namespace, label_selector=selector_str)

    return list(filter(lambda pod: target.matches(pod), pods.items))


def parse_config(config: Dict[str, List[object]]) -> Dict[str, Dict[str, object]]:
    targets = [ConfigTarget.model_validate(targ) for targ in config.get("targets", [])]
    targets_dict = {target.name: target for target in targets}
    assert_unique_attr(targets, "name")

    endpoints = [ConfigEndpoint.model_validate(endpoint) for endpoint in config["endpoints"]]
    endpoints_dict = {endpoint.name: endpoint for endpoint in endpoints}
    assert_unique_attr(endpoints, "name")

    requests = []
    for request_dict in config["requests"]:
        request_dict["endpoint"] = endpoints_dict[request_dict["endpoint"]]
        requests.append(ConfigRequest.model_validate(request_dict))
    requests_dict = {request.name: request for request in requests}
    assert_unique_attr(requests, "name")

    actions: List[ConfigAction] = []
    for action in config["actions"]:
        try:
            action["requests"] = [requests_dict[req] for req in action["requests"]]
        except KeyError as e:
            raise ValueError(
                f"Action contains unknown request. action: `{action}` requests: `{requests_dict}`"
            ) from e
        try:
            action["targets"] = [targets_dict[targ] for targ in action["targets"]]
        except KeyError as e:
            raise ValueError(
                f"Action contains unknown target. action: `{action}` targets: `{targets_dict}`"
            ) from e
        actions.append(ConfigAction.model_validate(action))
    actions_dict = {action.name: action for action in actions}
    assert_unique_attr(actions, "name")

    return {
        "targets": targets_dict,
        "endpoints": endpoints_dict,
        "requests": requests_dict,
        "actions": actions_dict,
    }


def load_configs(config_files: List[str]) -> Dict[str, Dict[str, object]]:
    logger.info(f"Loading configs: {config_files}")
    full_config = defaultdict(list)
    for config_file in config_files:
        with open(config_file, "r") as file:
            config = yaml.safe_load(file)
            for key, value in config.items():
                full_config[key].extend(value)
    return parse_config(full_config)


def do_action(
    action: ConfigAction,
    pods: List[TargetPodInfo],
) -> List[TargetPodInfo]:
    target_names = [target.name for target in action.targets]
    possible_pods = [pod for pod in pods if pod.config_target.name in target_names]

    if action.order == "random":
        random.shuffle(possible_pods)
    elif action.order == "ascending":
        possible_pods.sort(key=lambda pod: pod.pod_name)
    elif action.order == "descending":
        possible_pods.sort(key=lambda pod: pod.pod_name, reverse=True)
    else:
        raise ValueError(f"Unknown order for action: {action.order}")

    pods = []
    count = len(possible_pods) if action.pod_count == "all" else action.pod_count
    index = action.pod_start_index
    for _ in range(count):
        pods.append(possible_pods[index])
        index = (index + 1) % len(possible_pods)

    if action.loop_order == "foreach_pod_make_all_requests":
        for pod in pods:
            for request in action.requests:
                # time.sleep(delay_between_requests) TODO
                call_endpoint(request, pod)
    elif action.loop_order == "foreach_request_target_each_pod":
        for request in action.requests:
            # TODO: ensure time between requests has elapsed
            for pod in pods:
                call_endpoint(request, pod)
    else:
        raise ValueError(f"Unknown loop_order for action: {action}")


def get_pod_infos(targets: List[ConfigTarget]) -> List[TargetPodInfo]:
    pods_info: List[TargetPodInfo] = []
    for target in targets:
        pods = get_pods_for_target(target)
        for pod in pods:
            pods_info.append(TargetPodInfo(config_target=target, pod=pod))
    return pods_info


def create_app(config) -> FastAPI:
    app = FastAPI()

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
    # TODO: Implement try/catch return error in decorator. It will be the same for all endpoints.
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
                pod_info = next(iter(get_pod_infos([target])))
            except StopIteration as e:
                raise NotFoundError(f"Target not found. Target: {target}") from e
            result = call_endpoint(request.endpoint, pod_info)
            return result
        except Exception as e:
            logger.error(HTTPException(status_code=500, detail=f"{e!r}\n{traceback.format_exc()}"))
            raise HTTPException(status_code=500, detail=f"{e!r}\n{traceback.format_exc()}") from e


def main(args: Namespace):
    config = load_configs(args.config_files)
    available_endpoints = [endpoint.name for endpoint in config["endpoints"].values()]
    logger.debug(f"Loaded config. Available endpoints: {available_endpoints}")
    if args.mode == "server":
        app = create_app(config)
        uvicorn.run(app, host="0.0.0.0", port=args.port, log_config=None)
    else:
        pods_info = get_pod_infos(config["targets"])
        for action in config["actions"]:
            do_action(action, pods_info)


def mode_type(value):
    if value not in ["batch", "server"]:
        raise argparse.ArgumentTypeError(f"Invalid mode: {value}. Must be 'batch' or 'server'.")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Waku storage retriever")
    parser.add_argument(
        "--config",
        type=Path,
        action="append",
        dest="config_files",
        required=True,
        help="Paths to config files. Can be passed multiple times.",
    )
    parser.add_argument(
        "--mode",
        type=mode_type,
        default="server",
        help="Batch: Run actions immediately. Server: Wait for API calls to /action/<myaction> to run.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8645,
        help="Port for the action HTTP server (default 8000)",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)

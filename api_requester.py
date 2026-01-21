import argparse
import random
from argparse import Namespace
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import uvicorn
import yaml

from app import create_app
from common import call_endpoint, get_pod_infos
from configs import ConfigAction, ConfigEndpoint, ConfigRequest, ConfigTarget
from schemas import TargetPodInfo
from utils import setup_logger

logger = setup_logger(__file__)


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
):
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
        help="Port for the action HTTP server (default 8645)",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)

import traceback
from typing import List, Optional

import requests

from configs import ConfigEndpoint, ConfigRequest, ConfigTarget
from kube_client import core_v1
from schemas import TargetPodInfo
from utils import paged_request, setup_logger

logger = setup_logger(__file__)


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
                result = requests.get(
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


def get_pod_infos(
    targets: List[ConfigTarget], *, namespace: Optional[str] = None
) -> List[TargetPodInfo]:
    pods_info: List[TargetPodInfo] = []
    for target in targets:
        pods = get_pods_for_target(target, namespace=namespace)
        for pod in pods:
            pods_info.append(TargetPodInfo(config_target=target, pod=pod))
    return pods_info


def get_pods_for_target(target: ConfigTarget, *, namespace: Optional[str] = None) -> List[str]:
    if not namespace:
        namespace = (
            open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read() or "default"
        )

    if target.service:
        pods = core_v1.list_namespaced_pod(namespace)
    else:
        service = core_v1.read_namespaced_service(target.service, namespace)
        selector = service.spec.selector
        selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
        pods = core_v1.list_namespaced_pod(namespace, label_selector=selector_str)

    return list(filter(lambda pod: target.matches(pod, namespace=namespace), pods.items))

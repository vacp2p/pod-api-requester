import datetime
import logging
import re
from typing import List, Literal, Optional

from kubernetes import client
from kubernetes.client.models.v1_pod import V1Pod
from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt, PositiveInt


class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Get UTC time and format with milliseconds
        utc_dt = datetime.datetime.utcfromtimestamp(record.created)
        if datefmt:
            s = utc_dt.strftime(datefmt)
            # Add milliseconds
            s = s + f".{int(record.msecs):03d}"
            return s
        else:
            t = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
            s = f"{t}.{int(record.msecs):03d}"
            return s


logfmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"

handler = logging.StreamHandler()
handler.setFormatter(UTCFormatter(logfmt, datefmt=datefmt))

logging.basicConfig(level=logging.INFO, handlers=[handler])

logger = logging.getLogger(__file__)


class ConfigEndpoint(BaseModel):
    """Describes an endpoint on a pod in the cluster.
    This endpoint may exist on multiple pods, or just a single pod, or no pod at all.

    It is the responsibility of the caller to combine a defined endpoint with a proper pod."""

    name: str
    """The name of this config object."""

    headers: dict
    """
    The header to send with the request.

    Typically either
    headers: {"accept": "text/plain"}
    or
    {"Content-Type": "application/json"}
    """

    params: dict
    """HTTP POST data to include with request."""

    url: str
    """Url for the endpoint.
    Instances of `{node}` and `{port}` will be replaced with
    the pod IP and the target port respectively.

    For example, when calling the following endpoint:
    `http://{node}:{port}/lightpush/v3/message`
    on the node at index `2` of the a target with:
    `stateful_set: "client", port: 8645`, then the following url will be used:
    `http://client-2:8645/lightpush/v3/message`
    """

    type: Literal["POST", "GET"]
    """Specifies the method of the request. Either `POST` or `GET`."""

    paged: bool
    """Use `True` if the request returns paged data. Otherwise, use `False`."""


class ConfigRequest(BaseModel):
    """A request to be made to a pod.
    Contains the `endpoint` and some additional data for retries/delays."""

    name: str
    """The name of this config object."""

    endpoint: ConfigEndpoint
    """The Endpoint to use for this request."""

    retries: NonNegativeInt
    """Number of times to retry the request if it fails."""

    retry_delay: NonNegativeFloat
    """The delay between each retry attempt for this request."""


class ConfigTarget(BaseModel):
    """A config describing pods.
    This is a list of filters to apply to any pod
    to see if that pod is part of the target group.
    """

    name: str
    """The name of this config object. Not the pod name."""

    service: Optional[str] = None
    """The name of the service that any target pod must belong.
    Example: zerotesting-bootstrap"""

    name_template: Optional[str] = None
    """Regex describing the pod names. Example: ^client-([0-9])$"""

    stateful_set: Optional[str] = None
    """Name of the StatefulSet that any target pod must belong to.
    Example: "bootstrap"
    """

    port: NonNegativeInt = 80
    """Port to use for requests to endpoints with this target.
    Default is 80."""

    def matches(self, pod: V1Pod) -> bool:
        """Check if pod is a valid target of self"""

        if self.stateful_set is not None:
            if pod.metadata.owner_references is None:
                return False
            if not all(
                [
                    owner.kind == "StatefulSet" and owner.name == self.stateful_set
                    for owner in pod.metadata.owner_references
                ]
            ):
                return False

        if self.name_template is not None:
            if not re.search(self.name_template, pod.metadata.name):
                return False

        if self.service is not None:
            v1 = client.CoreV1Api()
            namespace = (
                open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read() or "default"
            )
            service = v1.read_namespaced_service(self.service, namespace)
            selector = service.spec.selector
            if not all([pod.metadata.labels.get(key) == value for key, value in selector.items()]):
                return False

        return True


class ConfigAction(BaseModel):
    """Description of an action to take. Here is how an action is performed:
    1. For each ConfigTarget, add all pods to the list.
    2. Sort the list of pods according to `order`.
    3. Starting at `pod_start_index`, take `pod_count` pods.
    4. According to `loop_order`, make every request in `requests` to every pod in the remaining list.
    """

    name: str
    """The name of this config object."""

    loop_order: Literal["foreach_pod_make_all_requests", "foreach_request_target_each_pod"]
    """Which algorithm to use to determine how requests should be made to pods.

    `foreach_pod_make_all_requests`: Loop through the list of pods.
    At each pod, make all the requests in `requests`

    `foreach_request_target_each_pod`: Loop through the `requests` list.
    For each `ConfigRequest`, execute that request on all pods in the
    list of pods derived from the algorithm described above.
    """

    pod_start_index: NonNegativeInt = 0
    """Allows a user to "skip" a certain amount of pods.
    This is applied to a list created by combining the lists of pods from `targets`,
    and sorting the list according to `order`.

    Assumes that `pod_start_index < len(all_pods)`.
    """

    pod_count: PositiveInt | Literal["all"] = "all"
    """The number of pods for this action.

    This can be used to limit the total number of pods considered for requests.
    Like, `pod_start_index`, this applies to the list of pods created via combining
    pods from `targets` and sorting them.

    If `pod_count` is `"all"`, then all pods will be used. This will not deduplicate any pods in the list.
    If `pod_count > len(all_pods)`, then the cursor will loop back to the beginning of the list and continue
    adding pods until the list of pods to use has exactly `pod_count` elements in it.
    """

    order: Literal["ascending", "descending", "random"] | None
    """Once the list of possible pods is gathered by combining the lists of pods for each `ConfigTarget`,
    they will be sorted by this ordering before applying `pod_start_index` and `pod_count`.
    """

    targets: List[ConfigTarget]
    """A list of all `ConfigTarget`s used to gather the list of pods.
    For each target, all pods will be added to as potential targets.
    Then the list will be sorted according to `order`, and spliced
    according to `pod_start_index` and `pod_count`.

    Note: A pod may match multiple `ConfigTarget`s. In this case,
    the pod will be added to the list as many times as it matches.

    For example, with `ConfigTarget`s {"stateful_set": "some_pod"} and {"name_template": "^some_pod-[1-2]$"},
    where the StatefulSet of some_pod has `replicas: 4`, the list of pods to use would be:

    ["some_pod-0",  "some_pod-1", "some_pod-2", "some_pod-3", "some_pod-1", "some_pod-2"]

    which then may be sorted by `ascending` to look like:

    ["some_pod-0",  "some_pod-1", "some_pod-1", "some_pod-2", "some_pod-2", "some_pod-3"]

    then, the list would be spliced using `all_pods[pod_start_index:pod_start_index+pod_count]`,
    assuming that `pod_start_index+pod_count < len(all_pods)`.
    """

    requests: List[ConfigRequest]
    """The list of requests to do for each pod that ends up in the list of pods to request to.
    Every request will be executed, but it may not be on all pods matching every `ConfigTarget` in `targets`. See `targets`."""

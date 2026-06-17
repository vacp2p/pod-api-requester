from typing import Optional

from kubernetes.client.models.v1_pod import V1Pod
from pydantic import BaseModel, ConfigDict

from configs import ConfigTarget


class TargetPodInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config_target: ConfigTarget

    pod: Optional[V1Pod] = None
    """The Kubernetes pod object. Set for k8s-resolved targets."""

    host: Optional[str] = None
    """Hostname for statically/DNS-resolved targets (e.g. the Shadow host
    "pod-3"). Set instead of `pod` when the target did not come from the
    Kubernetes API. See shadow_resolver.py."""

    @property
    def pod_name(self) -> str:
        if self.pod is not None:
            return self.pod.metadata.name
        if self.host is not None:
            return self.host
        raise ValueError("TargetPodInfo has neither `pod` nor `host` set.")

    @property
    def node_address(self) -> str:
        """Address substituted into an endpoint URL's `{node}`: the pod IP for
        k8s targets, or the hostname (resolved by the HTTP client's OS resolver)
        for static targets."""
        if self.pod is not None:
            return self.pod.status.pod_ip
        if self.host is not None:
            return self.host
        raise ValueError("TargetPodInfo has neither `pod` nor `host` set.")


class NotFoundError(LookupError):
    """Raised when no object matches the given criteria.

    For example specifying a target name that does not exist in the ConfigMap."""

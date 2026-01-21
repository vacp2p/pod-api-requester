from kubernetes.client.models.v1_pod import V1Pod
from pydantic import BaseModel, ConfigDict

from configs import ConfigTarget


class TargetPodInfo(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config_target: ConfigTarget
    pod: V1Pod

    @property
    def pod_name(self) -> str:
        return self.pod.metadata.name


class NotFoundError(LookupError):
    """Raised when no object matches the given criteria.

    For example specifying a target name that does not exist in the ConfigMap."""

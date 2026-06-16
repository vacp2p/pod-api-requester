"""Static / DNS-based target resolution (no Kubernetes).

In environments without a Kubernetes control plane -- notably the Shadow
network simulator -- peers are not k8s pods: they are simulated hosts named by
convention (``pod-0`` .. ``pod-(N-1)``) and reachable only through the
simulator's internal DNS. This module resolves "static" ``ConfigTarget``s to
``TargetPodInfo`` objects from their explicit ``hosts`` list, with no calls to
the Kubernetes API.

Keeping this in its own module is deliberate: the non-k8s resolution path never
imports ``kube_client`` / the API client, so it works in a bare Python process
inside the simulator. It mirrors what the legacy ``traffic_sync.py`` injector
did (build ``pod-{i}`` names, let the OS resolver -- Shadow's DNS -- map them),
but expressed through the requester's config model.
"""

from typing import List

from configs import ConfigTarget
from schemas import TargetPodInfo
from utils import setup_logger

logger = setup_logger(__file__)


def resolve_static_target(target: ConfigTarget) -> List[TargetPodInfo]:
    """Resolve a static (non-k8s) target to ``TargetPodInfo`` objects by hostname.

    Each hostname in ``target.hosts`` is stored in ``TargetPodInfo.host``; it is
    substituted into the endpoint URL's ``{node}`` and resolved by the HTTP
    client's OS resolver (e.g. Shadow's internal DNS) at request time -- no IP
    lookup happens here.
    """
    if not target.hosts:
        raise ValueError(f"Static target `{target.name}` must set `hosts`.")
    hosts = list(target.hosts)
    preview = hosts[:5]
    logger.info(
        f"Resolved static target `{target.name}` to {len(hosts)} host(s): "
        f"{preview}{' ...' if len(hosts) > 5 else ''}"
    )
    return [TargetPodInfo(config_target=target, host=host) for host in hosts]

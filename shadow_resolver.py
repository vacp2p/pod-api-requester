"""Static / DNS-based target resolution (no Kubernetes).

In environments without a Kubernetes control plane -- notably the Shadow
network simulator -- peers are not k8s pods: they are simulated hosts named by
convention (``pod-0`` .. ``pod-(N-1)``) and reachable only through the
simulator's internal DNS. This module resolves "static" ``ConfigTarget``s to
``TargetPodInfo`` objects purely by expanding a hostname pattern, with no calls
to the Kubernetes API.

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


def expand_hosts(target: ConfigTarget) -> List[str]:
    """Expand a static target into the concrete list of hostnames it addresses.

    Two forms are supported:
      * ``hosts``: an explicit list of hostnames.
      * ``host_template`` + ``host_count``: ``host_template`` is formatted with
        ``i`` over ``range(host_count)`` -- e.g. ``"pod-{i}"`` with
        ``host_count=100`` yields ``pod-0`` .. ``pod-99``.
    """
    if target.hosts is not None:
        return list(target.hosts)
    if target.host_template is not None:
        if target.host_count is None:
            raise ValueError(
                f"Static target `{target.name}` sets `host_template` but not `host_count`."
            )
        return [target.host_template.format(i=i) for i in range(target.host_count)]
    raise ValueError(
        f"Static target `{target.name}` must set either `hosts` or "
        f"`host_template` (+ `host_count`)."
    )


def resolve_static_target(target: ConfigTarget) -> List[TargetPodInfo]:
    """Resolve a static (non-k8s) target to ``TargetPodInfo`` objects by hostname.

    The hostname is stored in ``TargetPodInfo.host``; it is substituted into the
    endpoint URL's ``{node}`` and resolved by the HTTP client's OS resolver
    (e.g. Shadow's internal DNS) at request time -- no IP lookup happens here.
    """
    hosts = expand_hosts(target)
    preview = hosts[:5]
    logger.info(
        f"Resolved static target `{target.name}` to {len(hosts)} host(s): "
        f"{preview}{' ...' if len(hosts) > 5 else ''}"
    )
    return [TargetPodInfo(config_target=target, host=host) for host in hosts]

from functools import cache

from kubernetes import client, config


@cache
def get_core_v1():
    """Create a CoreV1Api client, memoized after the first call.

    Importing this module must not connect to Kubernetes. Connecting at import
    time breaks every non-cluster use of the requester. Client creation is deferred
    until a Kubernetes target is actually resolved.

    Prefers in-cluster config (running as a pod), falling back to a local
    kubeconfig so the requester can also be run against a cluster from outside.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CoreV1Api()

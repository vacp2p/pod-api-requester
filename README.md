## Waku Storage Retriever

This Python script facilitates arbitrary GET and POST requests
to pods in a Kubernetes cluster.
The script is designed to run inside a Docker container.

### Usage
The script can be run in one of two modes: `server` or `batch`.

#### Batch Mode

`python ./api_requester.py --mode batch --config /mount/config.yaml`

Runs all actions in the config sequentially, then exits. 

Add `--namespace <ns>` when any target is resolved through the Kubernetes API.
It can be omitted when every target is static, which is how the requester runs 
in environments without a Kubernetes control plane such as the Shadow network simulator.

See `examples/shadow_publish.yaml` for a complete batch config.

#### Server Mode

`python ./api_requester.py --mode server --config /mount/config.yaml`

Runs a server, allowing scripts to call API endpoints, causing this pod to make API requests to other pods.

See endpoints under `def create_app` in `api_requester.py` for usage details.

### Config Format

The ConfigMap in `config.yaml` defines the config objects.
Class definitions are in `configs.py`.

The idea is that a user can define various pieces of the config,
then combine them as needed. Each object stands independently, but
they work together when running an action or making a request.

Each config object has a name by which it can be referenced.
Some fields are optional.

Endpoints - Defines an API endpoint for a request. May set a `timeout` (seconds, default 30).
Targets - Defines a set of filters to use to determine if pods on a cluster are part of the target. May instead be resolved by hostname — see "Static targets".
Requests - Contains an Endpoint and some additional information for retries and delays.
Actions - Combines Targets and Requests into a defined action, representing a series of requests. May set a `delay` (seconds) to pace requests.

#### Static targets (no Kubernetes)

By default a Target is resolved through the Kubernetes API (by `service`,
`name_template`, and/or `stateful_set`). A Target can instead be resolved purely
by hostname via `hosts` — an explicit list, e.g. `["pod-0", "pod-1"]` — with no
Kubernetes API calls, useful for the Shadow simulator or any environment without
a control plane.

The hostname is substituted into the endpoint URL's `{node}` and resolved by the
HTTP client's OS resolver at request time. Static and Kubernetes targets can be
mixed in the same config.

#### How an Action is performed

1. For each ConfigTarget, add all pods to the list "all_pods". Note: No deduplication is done.
2. Sort the list of pods according to `order`.
3. Starting at `pod_start_index`, take `pod_count` pods. Note: Loop through the list if needed to get `pod_count` items.
4. According to `loop_order`, make every request in `requests` to every pod in the remaining list.

See docstrings in the `ConfigAction` class for more details.

### Files

```
api_requester.py    Main code that will run on the pod-api-requester pod
bind.yaml           Necessary Kubernetes Role + RoleBinding to give permission to list pods
config.yaml         Kubernetes config containing the definitions for Targets, Endpoints, Requests, and Actions

build.sh            Sample commands to build the Docker container
Dockerfile          File to build the pod-api-requester image

deployment.yaml     Sample pod for development/testing
client.py           Sample code to make API requests directed at a pod running this code
```


### Changelog

- `v3.1.0`:
  - Added static / DNS-based target resolution (`hosts`: an explicit hostname list),
    so targets can be addressed by hostname with no Kubernetes API calls (e.g. inside the
    Shadow simulator).
  - `--mode batch` no longer requires a Kubernetes connection when all targets are static:
    the in-cluster client is created lazily, and `--namespace` is optional in that case.
  - Added `delay` on Actions (pacing between requests) and `timeout` on Endpoints
    (per-request HTTP timeout, default 30s — previously requests could block indefinitely).
  - Fixed the batch-mode action path (target/action iteration, namespace passing) and made
    pod ordering numeric (`pod-2` before `pod-10`).
- `v3.0.0`:
  - Added **load test mode** for high-throughput async testing
  - New `LoadTestConfig` in actions with rate limiting and burst support
  - Added `async_client.py` for aiohttp-based requests
  - Added `/loadtest/*` API endpoints for server mode
- `v2.0.0`:
  - Changed to using a ConfigMap to define Targets, Endpoints, Requests, and Actions
  - Added server capability
  - Removed --debug mode logic
- `v1.0.1`:
  - Added `--debug` mode. Makes multiple API requests to each IP
  - Added `--select-types` mode
    - Use the node type flags to determine which nodes to use for API requests
    - All API requests will be made to all nodes specified
    - Accepted arguments are a number or `all` (eg. --store=1 --relay=all)
    - Without using this flag, the script will use the old behavior
      (randomly choose a node using zerotesting-service)
- `v1.0.0`:
  Initial version

---

### Load Test Mode

Actions can run in **load test mode** for high-throughput async testing.

#### Enabling Load Test Mode

Add a `load_test` section to any action in `config.yaml`:

```yaml
actions:
  - name: gossip-burst
    requests: ["gossip-publish"]
    targets: ["nim-nodes"]
    order: ascending
    loop_order: foreach_pod_make_all_requests
    load_test:
      enabled: true
      rate_per_pod: 128.0       # messages/second per pod
      messages_per_pod: 512     # total messages per pod
```

#### Load Test Configuration Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable load test mode |
| `rate_per_pod` | float | - | Messages per second per pod |
| `messages_per_pod` | int | - | Total messages per pod |
| `duration_seconds` | float | - | Run duration (alternative to messages_per_pod) |
| `burst_size` | int | - | Messages per burst |
| `burst_delay` | float | - | Delay between bursts |
| `parallel_workers` | bool | `true` | Run workers in parallel |
| `request_timeout` | float | `30.0` | HTTP request timeout |

#### HTTP API Endpoints (Server Mode)

```bash
# List actions with load_test enabled
GET /loadtest/actions

# Run a pre-configured load test action
POST /loadtest/run/{action_name}

# Run an inline load test
POST /loadtest/inline
```
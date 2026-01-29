import datetime
import logging
import socket
import time
import traceback
from pathlib import Path
from typing import Any, Collection, Dict, List, Tuple

import requests
from pydantic import BaseModel, Field, PositiveInt

LOGFMT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


class UTCFormatter(logging.Formatter):
    """Formatter that outputs UTC timestamps with milliseconds."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc)
        if datefmt is None:
            datefmt = DATEFMT
        base = dt.strftime(datefmt)
        return f"{base}.{int(record.msecs):03d}"


def setup_logger(name: str) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(UTCFormatter(LOGFMT, datefmt=DATEFMT))

    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    return logging.getLogger(name)


logger = setup_logger(__file__)


def get_ips_by_service(service: str) -> List[str]:
    try:
        _, _, ips = socket.gethostbyname_ex(service)
        return ips[0]
    except Exception as e:
        error = traceback.format_exc()
        logger.error(
            f"Failed to resolve dns. service: `{service}`, exception: `{e}`, error: {error}"
        )
        raise


class Target(BaseModel):
    pod_name: str
    ip: str
    service: str
    dns_name: str


class NodeType(BaseModel):
    name_template: str
    """Format string for node name. Eg. fserver-0-{index}"""
    service: str
    count_key: str
    namespace: str = Field(default="zerotesting")

    def dns_name(self, index: PositiveInt) -> str:
        """Return name for DNS lookup.
        <pod-name>.<headless-service-name>
        """
        return f"{self.get_node_name(index)}.{self.service}"

    def get_node_name(self, index: PositiveInt) -> str:
        return self.name_template.format(index=index)


node_types = [
    NodeType(
        name_template="store-0-{index}",
        service="zerotesting-store",
        count_key="store",
    ),
    NodeType(
        # Note the plural "nodes" with an 's'!
        # This is to match the name used in regression tests.
        name_template="nodes-0-{index}",
        service="zerotesting-service",
        count_key="relay",
    ),
    NodeType(
        name_template="fserver-0-{index}",
        service="zerotesting-filter",
        count_key="filter_server",
    ),
    NodeType(
        name_template="fclient-0-{index}",
        service="zerotesting-filter",
        count_key="filter_client",
    ),
    NodeType(
        name_template="lpserver-0-{index}",
        service="zerotesting-lightpush-server",
        count_key="lightpush_server",
    ),
    NodeType(
        name_template="lpclient-0-{index}",
        service="zerotesting-lightpush-client",
        count_key="lightpush_client",
    ),
    NodeType(
        name_template="bootstrap-{index}",
        service="zerotesting-bootstrap",
        count_key="bootstrap",
    ),
]


def get_ips_by_type(args: dict, *, namespace=None) -> List[Tuple[str, str]]:
    """
    Get node ips based on type flags (--store, --relay, etc) starting at start_index for each node type.

    :return: (name, ip) tuples for node specified.
    :rtype: List[str, str]
    """
    # TODO: Handle multiple shards.

    results = []
    for node_type in node_types:
        start_index = args.get("start_index", 0)
        if args[node_type.count_key] == "all":
            try:
                _, _, ip_list = socket.gethostbyname_ex(node_type.service)
                count = len(ip_list) - start_index
            except socket.gaierror:
                # This happens when either:
                # 1. The service doesn't exist.
                # 2. No pods with the matching app selector exist, thus though the service exists, it isn't running on any pod.
                count = 0
            # TODO: Check at the end if all `count` ips have been found.
            # TODO: Add "unknown-{index}" for ips not in {nodetype}-0-{index}
            # Note that if node types share the same service, count will be set to the total.
            # for example fserver/fclient both use zerotesting-filter.
        else:
            try:
                count = int(args[node_type.count_key])
            except (KeyError, TypeError):
                logger.info(f"No count for nodetype specified. `{node_type}`")
                continue

        logger.info(
            f"Getting {count} IPs from nodes of type `{node_type.name_template}` starting at index {start_index}"
        )
        for index in range(start_index, start_index + count):
            dns = node_type.dns_name(index)
            try:
                _, _, ips = socket.gethostbyname_ex(dns)
                results.append((node_type.get_node_name(index), ips[0]))
            except Exception as e:
                error = traceback.format_exc()
                logger.error(
                    f"Failed to resolve dns. dns: `{dns}`, node_type: `{node_type}`, exception: `{e}`, error: {error}"
                )

    return results


def resolve_dns(node: str) -> Tuple[str, str]:
    start_time = time.time()
    name, port = node.split(":")
    ip_address = socket.gethostbyname(name)
    entire_hostname = socket.gethostbyaddr(ip_address)
    hostname = entire_hostname[0].split(".")[0]
    elapsed = (time.time() - start_time) * 1000
    logger.info(f"{node} DNS Response took {elapsed} ms")
    logger.info(f"Talking with {hostname}, ip address: {ip_address}")

    return (entire_hostname, f"{ip_address}:{port}")


def get_ips(args) -> Tuple[str, str]:
    port = 8645
    if args.select_types:
        ips = get_ips_by_type(vars(args))
        logger.info(f"ips: ({len(ips)}): ```{ips}```")
        return [(name, f"{ip}:{port}") for name, ip in ips]
    else:
        service = f"zerotesting-service:{port}"
        return [resolve_dns(service)]


# TODO: Extraneous code? (unused)
def get_api_args(args_dict: dict) -> dict:
    """These are the arguments that should be passed on to the GET request for store messages."""
    return {
        key: value
        for key, value in args_dict.items()
        if key
        in [
            "contentTopics",
            "pubsubTopic",
            "pageSize",
            "cursor",
        ]
    }


def dict_extract(obj: dict, path: Path):
    def extract(obj: Any, parts: list, is_list=False):
        if isinstance(obj, list):
            results = []
            for item in obj:
                results.extend(extract(item, parts, is_list=True))
            return results
        if not parts:
            return [obj] if is_list else obj
        next_obj = obj[parts[0]]
        return extract(next_obj, parts[1:], is_list)

    return extract(obj, path.parts)


def next_cursor(data: Dict) -> str | None:
    cursor = data.get("paginationCursor")
    if not cursor:
        logger.info("No more messages")
        return None

    return cursor


def paged_request(request: dict, max_attempts: PositiveInt, page_request_delay: float) -> dict:
    """
    GET request with a "paged" param.

    :param request: Must contain "params":dict.
    """
    attempt_num = 1

    url = request["url"]
    all_messages = []
    pages_data = []
    params = request["params"]
    status_codes = []
    inner_status_codes = []
    while True:
        time.sleep(page_request_delay)

        logger.info(f"Making paged request. request: `{request}`, params=`{params}`")
        response = requests.get(url, headers=request["headers"], params=params)

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            data = response.text

        status_codes.append(response.status_code)
        pages_data.append(data)

        logger.info(f"response to paged request: `{response}`")
        if response.status_code != 200:
            logger.error(
                f"Error fetching paged data. status_code: `{response.status_code}` data: `{data}`"
            )
            break

        inner_status_codes.append(data["statusCode"])
        logger.info(f"Response data: `{data}`")

        if data["statusCode"] != 200:
            logger.info(
                f"inner_status_code != 200: status_code: `{data['statusCode']}`, attempt: `{attempt_num}`"
            )

            if attempt_num >= max_attempts:
                logger.info(f"Exhausted all attempts: `{attempt_num}`")
                break
            attempt_num += 1
            continue

        logger.info(f"inner_status_code == 200: attempt: `{attempt_num}`")
        if attempt_num > 1:
            logger.info("A previous attempt failed, but now it worked.")

        paged_data = dict_extract(data, request.get("extract_keys", Path()))
        logger.info(f"Retrieved {len(paged_data)} messages on attempt `{attempt_num}`")
        all_messages.extend(paged_data)

        cursor = next_cursor(data)
        if not cursor:
            logger.info(f"page request finished with !cursor on attempt `{attempt_num}`")
            break
        params["cursor"] = cursor

        attempt_num = 1

    logger.info("finished page request")
    return {
        "request": request,
        "response": {
            "statusCodes": status_codes,
            "inner_statusCodes": inner_status_codes,
            "messages": all_messages,
            "pages": pages_data,
            "attempt_num": attempt_num,
        },
    }


def redact_keys(
    obj: Any,
    keys_to_redact: Collection[str],
    replacement: Any = "<redacted>",
):
    """
    Return a copy of `obj` where any dict entry whose key is in keys_to_redact
    has its value replaced with `replacement`, recursively through dicts and
    lists/tuples.
    """
    if isinstance(obj, dict):
        new_dict = {}
        for key, value in obj.items():
            if key in keys_to_redact:
                new_dict[key] = replacement
            else:
                new_dict[key] = redact_keys(value, keys_to_redact, replacement)
        return new_dict
    elif isinstance(obj, list):
        return [redact_keys(item, keys_to_redact, replacement) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(redact_keys(item, keys_to_redact, replacement) for item in obj)
    else:
        return obj

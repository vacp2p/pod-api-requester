"""
Async HTTP client for load testing.

Provides high-throughput message injection with rate limiting,
burst mode, and parallel worker support.
"""

import asyncio
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

from configs import ConfigAction, ConfigEndpoint, ConfigRequest, LoadTestConfig
from schemas import TargetPodInfo
from utils import setup_logger

logger = setup_logger(__file__)


@dataclass
class RequestStats:
    """Thread-safe statistics tracking for load test requests."""

    success: int = 0
    failure: int = 0
    total: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record(self, ok: bool, latency_ms: float) -> None:
        async with self._lock:
            self.total += 1
            self.total_latency_ms += latency_ms
            if latency_ms < self.min_latency_ms:
                self.min_latency_ms = latency_ms
            if latency_ms > self.max_latency_ms:
                self.max_latency_ms = latency_ms
            if ok:
                self.success += 1
            else:
                self.failure += 1

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            success_rate = (self.success / self.total * 100.0) if self.total else 0.0
            avg_latency = (self.total_latency_ms / self.total) if self.total else 0.0
            return {
                "success": self.success,
                "failure": self.failure,
                "total": self.total,
                "success_rate": success_rate,
                "avg_latency_ms": avg_latency,
                "min_latency_ms": self.min_latency_ms if self.total else 0.0,
                "max_latency_ms": self.max_latency_ms,
            }


@dataclass
class WorkerContext:
    """Context for a per-pod worker."""
    worker_id: int
    pod_info: TargetPodInfo
    action: ConfigAction
    session: aiohttp.ClientSession
    stats: RequestStats


def build_request_url(endpoint: ConfigEndpoint, pod_info: TargetPodInfo) -> str:
    """Build the full URL for a request to a specific pod."""
    return endpoint.url.format(
        node=pod_info.pod.status.pod_ip,
        port=pod_info.config_target.port,
    )


async def send_async_request(
    ctx: WorkerContext,
    request: ConfigRequest,
    message_index: int,
) -> Dict[str, Any]:
    """Send a single async HTTP request and record stats."""
    endpoint = request.endpoint
    url = build_request_url(endpoint, ctx.pod_info)
    pod_name = ctx.pod_info.pod_name
    load_test = ctx.action.load_test

    result_data: Dict[str, Any] = {
        "request": {
            "url": url,
            "pod": pod_name,
            "worker_id": ctx.worker_id,
            "message_index": message_index,
        }
    }

    start_time = time.time()

    try:
        response_data = await _do_http_request(
            ctx.session, endpoint, url, load_test.request_timeout
        )

        elapsed_ms = (time.time() - start_time) * 1000.0
        ok = response_data["status_code"] == 200

        await ctx.stats.record(ok, elapsed_ms)
        snap = await ctx.stats.snapshot()

        result_data["response"] = response_data
        result_data["elapsed_ms"] = elapsed_ms
        result_data["stats"] = snap

        logger.info(
            "worker=%d message=%d pod=%s status=%d elapsed_ms=%.2f "
            "success=%d failure=%d total=%d success_rate=%.2f%%",
            ctx.worker_id,
            message_index,
            pod_name,
            response_data["status_code"],
            elapsed_ms,
            snap["success"],
            snap["failure"],
            snap["total"],
            snap["success_rate"],
        )

    except Exception as e:
        elapsed_ms = (time.time() - start_time) * 1000.0
        await ctx.stats.record(False, elapsed_ms)
        snap = await ctx.stats.snapshot()

        error_msg = traceback.format_exc()
        result_data["exception"] = str(e)
        result_data["traceback"] = error_msg
        result_data["elapsed_ms"] = elapsed_ms
        result_data["stats"] = snap

        logger.warning(
            "worker=%d message=%d pod=%s exception=%s elapsed_ms=%.2f "
            "success=%d failure=%d total=%d success_rate=%.2f%%",
            ctx.worker_id,
            message_index,
            pod_name,
            repr(e),
            elapsed_ms,
            snap["success"],
            snap["failure"],
            snap["total"],
            snap["success_rate"],
        )

    return result_data


async def _do_http_request(
    session: aiohttp.ClientSession,
    endpoint: ConfigEndpoint,
    url: str,
    timeout: float,
) -> Dict[str, Any]:
    """Perform the actual HTTP request."""
    timeout_obj = aiohttp.ClientTimeout(total=timeout)

    if endpoint.type == "POST":
        async with session.post(
            url,
            json=endpoint.params,
            headers=endpoint.headers,
            timeout=timeout_obj,
        ) as response:
            text = await response.text()
            return {"status_code": response.status, "text": text[:500]}
    elif endpoint.type == "GET":
        async with session.get(
            url,
            params=endpoint.params,
            headers=endpoint.headers,
            timeout=timeout_obj,
        ) as response:
            text = await response.text()
            return {"status_code": response.status, "text": text[:500]}
    else:
        raise ValueError(f"Unsupported request type: {endpoint.type}")


async def run_pod_worker(ctx: WorkerContext) -> Dict[str, Any]:
    """Worker that sends messages to a single pod at specified rate."""
    load_test = ctx.action.load_test
    requests_list = ctx.action.requests
    pod_name = ctx.pod_info.pod_name

    delay_seconds = load_test.get_delay_seconds()
    burst_size = load_test.burst_size or 1
    burst_delay = load_test.burst_delay or delay_seconds

    start_time = time.time()
    message_index = 0

    logger.info(
        "worker=%d pod=%s starting messages=%s duration=%s rate=%s",
        ctx.worker_id, pod_name, load_test.messages_per_pod,
        load_test.duration_seconds, load_test.rate_per_pod,
    )

    while True:
        if load_test.messages_per_pod is not None and message_index >= load_test.messages_per_pod:
            break
        if load_test.duration_seconds is not None and (time.time() - start_time) >= load_test.duration_seconds:
            break

        # Send burst
        burst_tasks = []
        for _ in range(burst_size):
            if load_test.messages_per_pod is not None and message_index >= load_test.messages_per_pod:
                break
            request = requests_list[message_index % len(requests_list)]
            burst_tasks.append(asyncio.create_task(send_async_request(ctx, request, message_index)))
            message_index += 1

        if burst_tasks:
            await asyncio.gather(*burst_tasks, return_exceptions=True)

        await asyncio.sleep(burst_delay if burst_size > 1 else delay_seconds)

    elapsed_total = time.time() - start_time
    logger.info(
        "worker=%d pod=%s finished messages=%d elapsed=%.2f",
        ctx.worker_id, pod_name, message_index, elapsed_total,
    )

    return {
        "worker_id": ctx.worker_id,
        "pod": pod_name,
        "messages_sent": message_index,
        "elapsed_seconds": elapsed_total,
    }


async def run_load_test(
    action: ConfigAction,
    pods: List[TargetPodInfo],
) -> Dict[str, Any]:
    """Execute a load test action against multiple pods."""
    load_test = action.load_test
    load_test.validate_config()

    if not pods:
        raise ValueError("No target pods for load test")

    stats = RequestStats()
    worker_results = []
    worker_errors = []

    start_time = time.time()
    connector = aiohttp.TCPConnector(ttl_dns_cache=300, limit=0)
    timeout = aiohttp.ClientTimeout(total=load_test.request_timeout)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if load_test.parallel_workers:
            workers = []
            for i, pod_info in enumerate(pods):
                ctx = WorkerContext(
                    worker_id=i,
                    pod_info=pod_info,
                    action=action,
                    session=session,
                    stats=stats,
                )
                workers.append((i, asyncio.create_task(run_pod_worker(ctx))))

            for worker_id, task in workers:
                try:
                    result = await task
                    worker_results.append(result)
                except Exception as e:
                    worker_errors.append({"worker_id": worker_id, "error": str(e)})
                    logger.error(f"Worker {worker_id} failed: {e}")
        else:
            for i, pod_info in enumerate(pods):
                ctx = WorkerContext(
                    worker_id=i,
                    pod_info=pod_info,
                    action=action,
                    session=session,
                    stats=stats,
                )
                try:
                    result = await run_pod_worker(ctx)
                    worker_results.append(result)
                except Exception as e:
                    worker_errors.append({"worker_id": i, "error": str(e)})
                    logger.error(f"Worker {i} failed: {e}")

    elapsed_total = time.time() - start_time
    final_stats = await stats.snapshot()

    summary = {
        "action": action.name,
        "pods_count": len(pods),
        "elapsed_seconds": elapsed_total,
        "stats": final_stats,
        "throughput_msg_per_sec": final_stats["total"] / elapsed_total if elapsed_total > 0 else 0,
        "worker_errors": worker_errors,
    }

    logger.info(
        "load_test_complete action=%s pods=%d total=%d success_rate=%.2f%% throughput=%.2f msg/s",
        action.name, len(pods), final_stats["total"],
        final_stats["success_rate"], summary["throughput_msg_per_sec"],
    )

    return summary

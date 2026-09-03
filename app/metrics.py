"""Prometheus Metrics & Telemetry Module (MS-009).

Maintains Prometheus-compatible counters, gauges, and latency histograms without external
heavy dependencies, exporting standard text/plain version 0.0.4 output.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class MetricsRegistry:
    """Thread-safe in-memory metric accumulator with bounded histogram storage."""

    def __init__(self, max_samples: int = 5000):
        self.counters: dict[str, dict[tuple, float]] = defaultdict(lambda: defaultdict(float))
        self.gauges: dict[str, dict[tuple, float]] = defaultdict(lambda: defaultdict(float))
        self.histogram_counts: dict[str, int] = defaultdict(int)
        self.histogram_sums: dict[str, float] = defaultdict(float)
        self.histograms: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=max_samples))

    def inc_counter(self, name: str, value: float = 1.0, **labels):
        label_key = tuple(sorted(labels.items()))
        self.counters[name][label_key] += value

    def set_gauge(self, name: str, value: float, **labels):
        label_key = tuple(sorted(labels.items()))
        self.gauges[name][label_key] = value

    def observe_histogram(self, name: str, value: float):
        self.histogram_counts[name] += 1
        self.histogram_sums[name] += value
        self.histograms[name].append(value)

    def generate_prometheus_text(self) -> str:
        lines: list[str] = []

        # Header comments & counter values
        for name, label_dict in self.counters.items():
            lines.append(f"# TYPE {name} counter")
            for labels, val in sorted(label_dict.items()):
                if labels:
                    label_str = ",".join(f'{k}="{v}"' for k, v in labels)
                    lines.append(f"{name}{{{label_str}}} {val}")
                else:
                    lines.append(f"{name} {val}")

        # Gauges
        for name, label_dict in self.gauges.items():
            lines.append(f"# TYPE {name} gauge")
            for labels, val in sorted(label_dict.items()):
                if labels:
                    label_str = ",".join(f'{k}="{v}"' for k, v in labels)
                    lines.append(f"{name}{{{label_str}}} {val}")
                else:
                    lines.append(f"{name} {val}")

        # Histograms / Summaries (bounded, using exact counts and sums)
        all_hist_names = sorted(set(self.histograms.keys()) | set(self.histogram_counts.keys()))
        for name in all_hist_names:
            count = self.histogram_counts[name]
            if count == 0:
                continue
            lines.append(f"# TYPE {name} summary")
            total = self.histogram_sums[name]
            lines.append(f"{name}_count {count}")
            lines.append(f"{name}_sum {total:.6f}")

        return "\n".join(lines) + "\n"


# Global singleton registry
metrics = MetricsRegistry()

# Initialize baseline platform counters
metrics.inc_counter("flux_http_requests_total", value=0, method="GET", status="200")
metrics.inc_counter("flux_ingest_accepted_total", value=0)
metrics.inc_counter("flux_chunk_uploads_total", value=0)
metrics.inc_counter("flux_dedup_merges_total", value=0)
metrics.inc_counter("flux_dedup_creates_total", value=0)
metrics.inc_counter("flux_dlq_errors_total", value=0)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Middleware measuring request count and execution latency."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Don't track /metrics to prevent metric inflation
        if request.url.path == "/metrics":
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        # Normalize path
        path = request.url.path
        if path.startswith("/api/uploads/"):
            path = "/api/uploads/*"
        elif path.startswith("/potholes/"):
            path = "/potholes/*"
        elif path.startswith("/detections/"):
            path = "/detections/*"

        metrics.inc_counter(
            "flux_http_requests_total",
            method=request.method,
            endpoint=path,
            status=str(response.status_code),
        )
        metrics.observe_histogram("flux_http_request_duration_seconds", duration)
        return response

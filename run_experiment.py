
"""
Repeatable MCP fault-injection experiment runner.

Uses the official Python MCP ClientSession to communicate with
the proxy and SDK-based upstream server.

Records:
- client-observed tool-call latency
- upstream response latency
- injected delay
- proxy processing intervals
- protocol outcome
- semantic correctness
- fault activation
- client trace completeness
- proxy lifecycle
- response withholding and delivery outcomes

After a client tool-call timeout, the runner keeps the MCP
transport alive for a configurable drain period. This allows
the proxy to finish delayed response tasks before shutdown.
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


CONDITIONS = [
    ("baseline", "configs/no_faults_sdk.json"),
    ("latency", "configs/latency_sdk.json"),
    ("error", "configs/error_sdk.json"),
    ("corruption", "configs/corruption_sdk.json"),
    ("hang", "configs/hang_sdk.json"),
]

EXPECTED_REVENUE = 120
TOOL_NAME = "get_company_revenue"


def load_trace_path(config):
    with open(config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    return Path(cfg.get("log_file", "logs/proxy.jsonl"))


def load_execution_trace(trace_path, execution_id):
    events = []

    if not trace_path.exists():
        return events

    with trace_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            if event.get("execution_id") == execution_id:
                events.append(event)

    return events


def get_tool_request_id(events):
    """Discover the request ID from the actual tools/call event."""

    for event in events:
        if (
            event.get("event") == "message_received"
            and event.get("method") == "tools/call"
            and event.get("tool") == TOOL_NAME
        ):
            return event.get("request_id")

    return None


def find_event(events, event_name, request_id):
    for event in events:
        if (
            event.get("event") == event_name
            and event.get("request_id") == request_id
        ):
            return event

    return None


def analyze_trace(events, protocol_outcome=None):
    """Derive timing, fault, trace, and proxy lifecycle metrics."""

    request_id = get_tool_request_id(events)

    if request_id is None:
        return {
            "trace_status": "incomplete",
            "proxy_lifecycle_status": "not_started",
            "fault_activated": False,
            "fault_type_observed": None,
            "corruption_applied": False,
            "corruption_target_not_found": False,
            "request_received": False,
            "request_forwarded": False,
            "upstream_response_received": False,
            "response_forwarded": False,
            "synthetic_response_sent": False,
            "response_withholding_started": False,
            "response_withholding_completed": False,
            "response_forwarding_failed": False,
            "pre_forward_seconds": None,
            "injected_delay_seconds": None,
            "upstream_response_seconds": None,
            "post_response_proxy_seconds": None,
            "proxy_lifecycle_seconds": None,
        }

    received = find_event(events, "message_received", request_id)

    forwarded = find_event(
        events, "request_forwarded", request_id
    )

    upstream_response = find_event(
        events, "upstream_response_received", request_id
    )

    response_forwarded = find_event(
        events, "response_forwarded", request_id
    )

    synthetic_response = find_event(
        events, "synthetic_response_sent", request_id
    )

    fault = find_event(
        events, "fault_injected", request_id
    )

    corruption = find_event(
        events, "corruption_applied", request_id
    )

    corruption_missing = find_event(
        events, "corruption_target_not_found", request_id
    )

    withholding_started = find_event(
        events, "response_withholding_started", request_id
    )

    withholding_completed = find_event(
        events, "response_withholding_completed", request_id
    )

    forwarding_failed = find_event(
        events, "response_forwarding_failed", request_id
    )

    def interval_seconds(start, end):
        if start is None or end is None:
            return None

        return round(
            end["timestamp_unix"] - start["timestamp_unix"],
            4,
        )

    pre_forward_seconds = interval_seconds(
        received, forwarded
    )

    upstream_response_seconds = interval_seconds(
        forwarded, upstream_response
    )

    post_response_seconds = interval_seconds(
        upstream_response, response_forwarded
    )

    # For hang faults, measure the complete proxy lifecycle
    # through withholding completion or delivery.
    lifecycle_end = (
        response_forwarded
        or withholding_completed
        or synthetic_response
    )

    proxy_lifecycle_seconds = interval_seconds(
        received, lifecycle_end
    )

    injected_delay_seconds = (
        interval_seconds(fault, forwarded)
        if fault and fault.get("fault_type") == "latency"
        else None
    )

    # Client-facing trace completeness.
    expected_trace_events = (
        received is not None
        and (
            synthetic_response is not None
            or (
                forwarded is not None
                and upstream_response is not None
                and response_forwarded is not None
            )
        )
    )

    # A timeout remains a client timeout even if the proxy
    # subsequently completes its delayed response task.
    client_timeout_after_upstream = (
        protocol_outcome == "timeout"
        and fault is not None
        and fault.get("fault_type") == "hang"
        and forwarded is not None
        and upstream_response is not None
        and withholding_started is not None
    )

    if client_timeout_after_upstream:
        trace_status = "client_timeout_after_upstream_response"

    elif expected_trace_events:
        trace_status = "complete"

    else:
        trace_status = "incomplete"

    # Proxy lifecycle and client delivery are independent.
    if synthetic_response is not None or response_forwarded is not None:
        proxy_lifecycle_status = "completed"

    elif forwarding_failed is not None:
        proxy_lifecycle_status = (
            "withholding_completed_delivery_failed"
        )

    elif withholding_completed is not None:
        proxy_lifecycle_status = (
            "withholding_completed_delivery_unknown"
        )

    elif withholding_started is not None:
        proxy_lifecycle_status = "response_withholding_incomplete"

    elif upstream_response is not None:
        proxy_lifecycle_status = "upstream_response_received"

    elif forwarded is not None:
        proxy_lifecycle_status = "request_forwarded"

    elif received is not None:
        proxy_lifecycle_status = "request_received"

    else:
        proxy_lifecycle_status = "not_started"

    return {
        "trace_status": trace_status,
        "proxy_lifecycle_status": proxy_lifecycle_status,
        "fault_activated": fault is not None,
        "fault_type_observed": (
            fault.get("fault_type") if fault else None
        ),
        "corruption_applied": corruption is not None,
        "corruption_target_not_found": corruption_missing is not None,
        "request_received": received is not None,
        "request_forwarded": forwarded is not None,
        "upstream_response_received": upstream_response is not None,
        "response_forwarded": response_forwarded is not None,
        "synthetic_response_sent": synthetic_response is not None,
        "response_withholding_started": withholding_started is not None,
        "response_withholding_completed": withholding_completed is not None,
        "response_forwarding_failed": forwarding_failed is not None,
        "pre_forward_seconds": pre_forward_seconds,
        "injected_delay_seconds": injected_delay_seconds,
        "upstream_response_seconds": upstream_response_seconds,
        "post_response_proxy_seconds": post_response_seconds,
        "proxy_lifecycle_seconds": proxy_lifecycle_seconds,
    }


def contains_timeout_exception(exc):
    """Detect timeout exceptions, including wrapped exceptions."""

    visited = set()
    current = exc

    while current is not None and id(current) not in visited:
        visited.add(id(current))

        if isinstance(current, (asyncio.TimeoutError, TimeoutError)):
            return True

        if type(current).__name__ in (
            "TimeoutError",
            "Timeout",
            "TimeoutException",
        ):
            return True

        current = (
            current.__cause__
            if current.__cause__ is not None
            else current.__context__
        )

    return False


async def one_run_async(
    config,
    execution_id,
    response_timeout=10,
    tool_timeout=3,
    proxy_drain_timeout=6,
):
    session_started = time.perf_counter()
    trace_path = load_trace_path(config)

    call_response = None
    tool_elapsed = None
    runner_error = None
    runner_stage = "proxy_startup"
    timeout_occurred = False
    proxy_drain_elapsed = 0.0

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
            "proxy.py",
            "--config",
            config,
            "--execution-id",
            execution_id,
        ],
    )

    try:
        async with stdio_client(server_params) as (
            read_stream,
            write_stream,
        ):
            async with ClientSession(read_stream, write_stream) as session:

                runner_stage = "initialization"

                await asyncio.wait_for(
                    session.initialize(),
                    timeout=response_timeout,
                )

                runner_stage = "tool_discovery"

                tools = await asyncio.wait_for(
                    session.list_tools(),
                    timeout=response_timeout,
                )

                available_tools = [
                    tool.name for tool in tools.tools
                ]

                if TOOL_NAME not in available_tools:
                    raise RuntimeError(
                        f"Required tool '{TOOL_NAME}' not found. "
                        f"Available tools: {available_tools}"
                    )

                runner_stage = "tool_call"
                tool_started = time.perf_counter()

                try:
                    result = await asyncio.wait_for(
                        session.call_tool(
                            TOOL_NAME,
                            arguments={},
                        ),
                        timeout=tool_timeout,
                    )

                    tool_elapsed = time.perf_counter() - tool_started

                    content_items = []

                    for item in result.content:
                        text_value = getattr(item, "text", None)

                        if text_value is not None:
                            content_items.append({
                                "type": "text",
                                "text": text_value,
                            })

                    call_response = {
                        "result": {
                            "content": content_items,
                            "isError": result.is_error,
                        }
                    }

                except Exception as exc:
                    tool_elapsed = time.perf_counter() - tool_started

                    if contains_timeout_exception(exc):
                        timeout_occurred = True

                        # IMPORTANT:
                        # Do not immediately close the MCP session.
                        # Keep the transport alive so the proxy can
                        # complete its delayed response task.
                        drain_started = time.perf_counter()

                        if proxy_drain_timeout > 0:
                            await asyncio.sleep(proxy_drain_timeout)

                        proxy_drain_elapsed = (
                            time.perf_counter() - drain_started
                        )

                    elif type(exc).__name__ in ("MCPError", "McpError"):
                        call_response = {
                            "error": {
                                "message": str(exc),
                                "exception_type": type(exc).__name__,
                            }
                        }

                    else:
                        raise

    except Exception as exc:
        if contains_timeout_exception(exc):
            timeout_occurred = True

        runner_error = (
            f"{runner_stage}: {type(exc).__name__}: {exc}"
        )

        if timeout_occurred:
            runner_error = (
                f"{runner_stage}: TimeoutError: {exc}"
            )

    session_elapsed = time.perf_counter() - session_started

    # Classify the client-observed protocol outcome.
    if call_response is None:
        if timeout_occurred:
            protocol_outcome = "timeout"
        else:
            protocol_outcome = "missing_response"

    elif "error" in call_response:
        protocol_outcome = "jsonrpc_error"

    elif "result" in call_response:
        protocol_outcome = "tool_result"

    else:
        protocol_outcome = "invalid_response"

    # Evaluate semantic correctness.
    observed_revenue = None

    if protocol_outcome == "tool_result":
        result = call_response["result"]

        for item in result.get("content", []):
            if item.get("type") == "text":
                try:
                    content = json.loads(item["text"])
                    observed_revenue = content.get("revenue")
                    break
                except (json.JSONDecodeError, TypeError):
                    pass

        if observed_revenue == EXPECTED_REVENUE:
            semantic_outcome = "correct_result"

        elif observed_revenue is not None:
            semantic_outcome = "incorrect_result"

        else:
            semantic_outcome = "unverifiable_result"

    elif protocol_outcome == "jsonrpc_error":
        semantic_outcome = "not_applicable"

    else:
        semantic_outcome = "not_evaluated"

    # Read the trace after the MCP session and proxy have shut down.
    events = load_execution_trace(trace_path, execution_id)

    trace_metrics = analyze_trace(
        events,
        protocol_outcome=protocol_outcome,
    )

    return {
        "execution_id": execution_id,
        "condition": config,
        "protocol_outcome": protocol_outcome,
        "semantic_outcome": semantic_outcome,
        "expected_revenue": EXPECTED_REVENUE,
        "observed_revenue": observed_revenue,
        "tool_call_elapsed_seconds": (
            round(tool_elapsed, 4)
            if tool_elapsed is not None
            else None
        ),
        "session_elapsed_seconds": round(session_elapsed, 4),
        "proxy_drain_elapsed_seconds": round(
            proxy_drain_elapsed, 4
        ),
        "trace_event_count": len(events),
        "runner_stage": runner_stage,
        "runner_error": runner_error,
        "tool_response": call_response,
        **trace_metrics,
    }


def one_run(
    config,
    execution_id,
    response_timeout=10,
    tool_timeout=3,
    proxy_drain_timeout=6,
):
    return asyncio.run(
        one_run_async(
            config,
            execution_id,
            response_timeout,
            tool_timeout,
            proxy_drain_timeout,
        )
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", default="results_sdk.jsonl")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--tool-timeout", type=float, default=3)

    parser.add_argument(
        "--proxy-drain-timeout",
        type=float,
        default=6,
        help="Seconds to keep the MCP session alive after a tool timeout.",
    )

    args = parser.parse_args()

    if args.runs < 1:
        parser.error("--runs must be >= 1")

    if args.timeout <= 0:
        parser.error("--timeout must be > 0")

    if args.tool_timeout <= 0:
        parser.error("--tool-timeout must be > 0")

    if args.proxy_drain_timeout < 0:
        parser.error("--proxy-drain-timeout must be >= 0")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    experiment_id = str(time.time_ns())

    with output.open("w", encoding="utf-8") as f:
        for condition, config in CONDITIONS:
            for i in range(1, args.runs + 1):

                execution_id = (
                    f"{condition}-{i:03d}-{experiment_id}"
                )

                result = one_run(
                    config,
                    execution_id,
                    args.timeout,
                    args.tool_timeout,
                    args.proxy_drain_timeout,
                )

                result["condition_name"] = condition
                result["experiment_id"] = experiment_id

                f.write(json.dumps(result) + "\n")
                f.flush()

                print(
                    condition,
                    i,
                    result["protocol_outcome"],
                    result["semantic_outcome"],
                    f'{result["tool_call_elapsed_seconds"] or 0:.3f}s',
                    f'trace={result["trace_status"]}',
                    f'proxy={result["proxy_lifecycle_status"]}',
                    f'withholding_completed={result["response_withholding_completed"]}',
                    f'forwarding_failed={result["response_forwarding_failed"]}',
                    f'drain={result["proxy_drain_elapsed_seconds"]:.2f}s',
                    f'fault={result["fault_activated"]}',
                    f'stage={result["runner_stage"]}',
                )


if __name__ == "__main__":
    main()

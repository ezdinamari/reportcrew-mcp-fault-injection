
"""
Minimal stdio-to-stdio MCP JSON-RPC fault-injection proxy.

Architecture:
    MCP client <-> proxy <-> upstream MCP server

Supported faults:
  - latency: delay forwarding a tools/call request
  - error: return a synthetic JSON-RPC error
  - corruption: modify a configured substring in a text result
  - hang: delay forwarding an upstream response

Delayed responses run independently of the upstream response-reading loop.
The proxy waits for pending response tasks during shutdown.

This is an assignment demonstrator, not a production MCP gateway.
"""

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    command = cfg.get("command", [])

    if command and command[0] == "$PYTHON":
        command[0] = sys.executable
        cfg["command"] = command

    cfg.setdefault("command", [sys.executable, "-m", "toy_server"])
    cfg.setdefault("faults", [])
    cfg.setdefault("log_file", "logs/proxy.jsonl")

    return cfg


class JsonlLogger:
    def __init__(self, path, execution_id):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.execution_id = execution_id

    def write(self, event, **fields):
        record = {
            "timestamp_unix": time.time(),
            "execution_id": self.execution_id,
            "event": event,
            **fields,
        }

        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


class FaultInjector:
    def __init__(self, faults, logger):
        self.faults = faults
        self.logger = logger
        self.calls_by_tool = defaultdict(int)

    def select_fault(self, tool_name):
        self.calls_by_tool[tool_name] += 1
        call_number = self.calls_by_tool[tool_name]

        for fault in self.faults:
            if fault.get("tool", "") not in ("", tool_name):
                continue

            every_n = int(fault.get("every_n", 1))
            offset = int(fault.get("offset", 0))

            if every_n < 1:
                raise ValueError("every_n must be >= 1")

            if (
                (call_number - offset) > 0
                and (call_number - offset) % every_n == 0
            ):
                return fault, call_number

        return None, call_number


async def write_line(writer, obj):
    line = json.dumps(obj, separators=(",", ":")) + "\n"

    if writer is sys.stdout:
        writer.write(line)
        writer.flush()
    else:
        writer.write(line.encode())
        await writer.drain()


async def run_proxy(config_path, execution_id):
    cfg = load_config(config_path)

    logger = JsonlLogger(cfg["log_file"], execution_id)
    injector = FaultInjector(cfg["faults"], logger)

    command = cfg["command"]

    if not isinstance(command, list) or not command:
        raise ValueError("'command' must be a non-empty list")

    child = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=None,
    )

    # Request ID -> fault metadata.
    pending = {}

    write_lock = asyncio.Lock()
    client_write_lock = asyncio.Lock()

    # Tasks handling delayed upstream responses.
    response_tasks = set()

    async def send_to_client(message, request_id, event_fields=None):
        """Write a response and record actual forwarding success."""

        try:
            async with client_write_lock:
                await write_line(sys.stdout, message)

        except (BrokenPipeError, OSError) as exc:
            logger.write(
                "response_forwarding_failed",
                request_id=request_id,
                error_type=type(exc).__name__,
                error=str(exc),
                **(event_fields or {}),
            )
            return False

        logger.write(
            "response_forwarded",
            request_id=request_id,
            **(event_fields or {}),
        )

        return True

    async def delayed_response(message, request_id, tool, delay):
        """
        Withhold an upstream response independently of the reader loop.

        Completion of the delay and successful delivery to the client
        are recorded as separate outcomes.
        """

        logger.write(
            "response_withholding_started",
            request_id=request_id,
            tool=tool,
            delay_seconds=delay,
        )

        try:
            await asyncio.sleep(delay)

            logger.write(
                "response_withholding_completed",
                request_id=request_id,
                tool=tool,
                delay_seconds=delay,
            )

            await send_to_client(
                message,
                request_id,
                event_fields={"tool": tool},
            )

        except asyncio.CancelledError:
            logger.write(
                "response_withholding_cancelled",
                request_id=request_id,
                tool=tool,
            )
            raise

        except Exception as exc:
            logger.write(
                "response_task_failed",
                request_id=request_id,
                tool=tool,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    def schedule_delayed_response(message, request_id, tool, delay):
        task = asyncio.create_task(
            delayed_response(
                message,
                request_id,
                tool,
                delay,
            )
        )

        response_tasks.add(task)
        task.add_done_callback(response_tasks.discard)

    async def client_to_server():
        loop = asyncio.get_running_loop()

        while True:
            line = await loop.run_in_executor(
                None,
                sys.stdin.buffer.readline,
            )

            if not line:
                break

            try:
                message = json.loads(line)

            except json.JSONDecodeError:
                logger.write(
                    "invalid_client_json",
                    raw=line.decode(errors="replace"),
                )
                continue

            request_id = message.get("id")
            method = message.get("method")
            params = message.get("params") or {}

            tool = (
                params.get("name")
                if method == "tools/call"
                else None
            )

            logger.write(
                "message_received",
                request_id=request_id,
                method=method,
                tool=tool,
            )

            if method == "tools/call":
                fault, call_number = injector.select_fault(tool)

                if fault:
                    kind = fault["type"]

                    common = {
                        "request_id": request_id,
                        "tool": tool,
                        "fault_type": kind,
                        "call_number_for_tool": call_number,
                    }

                    if kind == "error":
                        logger.write(
                            "fault_injected",
                            **common,
                        )

                        synthetic_response = {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {
                                "code": int(
                                    fault.get("error_code", -32001)
                                ),
                                "message": fault.get(
                                    "message",
                                    "Injected tool failure",
                                ),
                            },
                        }

                        sent = False

                        try:
                            async with client_write_lock:
                                await write_line(
                                    sys.stdout,
                                    synthetic_response,
                                )
                            sent = True

                        except (BrokenPipeError, OSError) as exc:
                            logger.write(
                                "synthetic_response_failed",
                                request_id=request_id,
                                error_type=type(exc).__name__,
                                error=str(exc),
                            )

                        if sent:
                            logger.write(
                                "synthetic_response_sent",
                                request_id=request_id,
                                response_kind="error",
                                tool=tool,
                            )

                        continue

                    elif kind == "latency":
                        delay = float(
                            fault.get("delay_seconds", 1.0)
                        )

                        logger.write(
                            "fault_injected",
                            delay_seconds=delay,
                            **common,
                        )

                        await asyncio.sleep(delay)

                    elif kind in ("corruption", "hang"):
                        pending[request_id] = {
                            "fault": fault,
                            "tool": tool,
                            "call_number": call_number,
                        }

                        logger.write(
                            "fault_injected",
                            **(
                                {
                                    **common,
                                    "delay_seconds": float(
                                        fault.get("delay_seconds", 5.0)
                                    ),
                                }
                                if kind == "hang"
                                else common
                            ),
                        )

                    else:
                        raise ValueError(
                            f"Unsupported fault type: {kind}"
                        )

            async with write_lock:
                logger.write(
                    "request_forwarded",
                    request_id=request_id,
                    method=method,
                    tool=tool,
                )

                child.stdin.write(line)
                await child.stdin.drain()

    async def server_to_client():
        while True:
            line = await child.stdout.readline()

            if not line:
                break

            try:
                message = json.loads(line)

            except json.JSONDecodeError:
                logger.write(
                    "invalid_server_json",
                    raw=line.decode(errors="replace"),
                )
                continue

            request_id = message.get("id")

            if "error" in message:
                response_kind = "error"
            elif "result" in message:
                response_kind = "result"
            else:
                response_kind = "other"

            logger.write(
                "upstream_response_received",
                request_id=request_id,
                response_kind=response_kind,
            )

            meta = (
                pending.pop(request_id, None)
                if request_id is not None
                else None
            )

            # Apply corruption if configured.
            if (
                meta
                and meta["fault"]["type"] == "corruption"
                and "result" in message
            ):
                fault = meta["fault"]

                target = fault.get(
                    "target_text",
                    '"revenue": 120',
                )

                replacement = fault.get(
                    "replacement_text",
                    '"revenue": 999',
                )

                changed = False
                result = message.get("result", {})

                for item in result.get("content", []):
                    if (
                        item.get("type") == "text"
                        and target in item.get("text", "")
                    ):
                        item["text"] = item["text"].replace(
                            target,
                            replacement,
                            1,
                        )
                        changed = True

                structured = result.get("structuredContent")

                if isinstance(structured, dict):
                    key = fault.get(
                        "structured_key",
                        "revenue",
                    )

                    if key in structured:
                        structured[key] = fault.get(
                            "replacement_value",
                            999,
                        )
                        changed = True

                logger.write(
                    (
                        "corruption_applied"
                        if changed
                        else "corruption_target_not_found"
                    ),
                    request_id=request_id,
                    tool=meta["tool"],
                    target_text=target,
                    replacement_text=replacement,
                    call_number_for_tool=meta["call_number"],
                )

            # Schedule delayed forwarding without blocking the reader.
            if meta and meta["fault"]["type"] == "hang":
                delay = float(
                    meta["fault"].get("delay_seconds", 5.0)
                )

                schedule_delayed_response(
                    message,
                    request_id,
                    meta["tool"],
                    delay,
                )

                continue

            # Normal or corrupted response.
            await send_to_client(
                message,
                request_id,
                event_fields={
                    "response_kind": response_kind,
                },
            )

    client_task = asyncio.create_task(client_to_server())
    server_task = asyncio.create_task(server_to_client())

    try:
        await client_task

    finally:
        # Stop accepting new requests from the client.
        if child.stdin and not child.stdin.is_closing():
            child.stdin.close()

        # Give the upstream process time to finish and close stdout.
        done, _ = await asyncio.wait(
            {server_task},
            timeout=2,
        )

        if not done:
            child.kill()

        await server_task

        # Crucial: wait for delayed responses even after client EOF.
        if response_tasks:
            await asyncio.gather(
                *list(response_tasks),
                return_exceptions=True,
            )

        # Ensure the upstream process is reaped.
        if child.returncode is None:
            try:
                await asyncio.wait_for(
                    child.wait(),
                    timeout=2,
                )

            except asyncio.TimeoutError:
                child.kill()
                await child.wait()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        default="configs/no_faults_sdk.json",
    )

    parser.add_argument(
        "--execution-id",
        default="demo-001",
    )

    args = parser.parse_args()

    asyncio.run(
        run_proxy(
            args.config,
            args.execution_id,
        )
    )


if __name__ == "__main__":
    main()

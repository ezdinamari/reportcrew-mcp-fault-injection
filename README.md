# ReportCrew MCP Fault-Injection Proxy

## 1. Overview

This project implements a stdio-to-stdio Model Context Protocol (MCP) proxy that intercepts communication between an MCP client and an upstream MCP server.

The proxy supports four controlled fault-injection mechanisms on `tools/call` requests:

1. **Latency injection:** delays forwarding a tool request by a configurable duration.
2. **Error injection:** returns a synthetic JSON-RPC error without forwarding the request upstream.
3. **Plausible result corruption:** modifies the upstream tool response while preserving a structurally successful MCP tool result.
4. **Response withholding (hang):** forwards a tool request, receives the upstream response, and deliberately withholds that response for a configurable duration.

The demonstrator uses the official MCP Python SDK for its server and client. A minimal MCP server exposes one tool, `get_company_revenue`, which returns ExampleCo's FY2025 revenue, with an expected value of 120 USD.

The objective is to demonstrate how faults at the MCP communication layer affect tool execution, protocol outcomes, and result correctness.

## 2. Architecture

The demonstrator consists of three main components:

- **MCP client (`test_mcp_server.py`):** establishes an MCP session through the proxy, discovers available tools, and invokes the revenue tool.
- **Fault-injection proxy (`proxy.py`):** intercepts JSON-RPC messages, applies configured faults, forwards requests, and records lifecycle events.
- **Upstream MCP server (`mcp_server.py`):** implements the `get_company_revenue` tool using the MCP Python SDK.

The experiment runner (`run_experiment.py`) automates repeated executions across the baseline and fault conditions, collects measurements, and classifies protocol and semantic outcomes.

Communication path:

`MCP client → fault-injection proxy → upstream MCP server`

Responses follow the reverse path through the proxy before reaching the client.

## 3. Requirements

- Python 3.10+
- MCP Python SDK version 2.3.0, specified in `requirements.txt`

Create and activate a virtual environment:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

No additional third-party dependencies are required beyond the official MCP SDK and its installed dependencies.

## 4. Running the demonstrator

### Individual condition tests

The client can be run through the proxy with the following configurations:

```bash
python test_mcp_server.py --config configs/no_faults_sdk.json
python test_mcp_server.py --config configs/latency_sdk.json
python test_mcp_server.py --config configs/error_sdk.json
python test_mcp_server.py --config configs/corruption_sdk.json
```

For the complete experiment, including the response-withholding condition, use the experiment runner:

```bash
python run_experiment.py --runs 3 --output results_submission_v2.jsonl
```

The runner executes the baseline and four fault conditions, producing 15 execution records: three repetitions per condition.

Expected behavior:

| Condition | Expected behavior |
|---|---|
| Baseline | Returns the original revenue: 120 |
| Latency | Returns 120 after an additional delay |
| Error | Returns an injected JSON-RPC error |
| Corruption | Returns a modified revenue: 999 |
| Hang | Client times out while the proxy withholds the upstream response |

The corruption condition illustrates an important distinction: a tool call can complete successfully at the protocol level while returning semantically incorrect information.

The hang condition illustrates that a client timeout does not necessarily mean the proxy's response-handling lifecycle has terminated.

The hang experiment uses a client tool-call timeout of approximately three seconds and a proxy withholding duration of approximately five seconds. The runner allows an additional drain period so that the proxy lifecycle can be observed after the client timeout.

## 5. Fault configuration

Each configuration file specifies:

- `command`: command used to launch the upstream MCP server.
- `log_file`: destination for the proxy's JSONL event log.
- `faults`: list of fault-injection rules.

Supported fault parameters include:

| Parameter | Description |
|---|---|
| `type` | Fault type: `latency`, `error`, `corruption`, or `hang` |
| `tool` | Target tool name, or `*` for all tools |
| `every_n` | Inject a fault on every Nth matching tool call |
| `offset` | Deterministic schedule offset |
| `delay_seconds` | Delay or withholding duration |
| `error_code` | JSON-RPC error code |
| `message` | Error message returned to the client |
| `target_text` | Text to locate in the tool response |
| `replacement_text` | Replacement text used for corruption |

The SDK configurations use `$PYTHON` as the upstream interpreter placeholder. The proxy resolves it to the currently running Python interpreter.

A fresh experiment identifier associates trace events with the corresponding experiment batch. Each execution also has its own execution ID.

## 6. Logging and measurements

The proxy records metadata-level lifecycle events in JSONL format, including:

- `message_received`
- `request_forwarded`
- `upstream_response_received`
- `response_forwarded`
- `fault_injected`
- `synthetic_response_sent`
- `corruption_applied`
- `corruption_target_not_found`
- `response_withholding_started`
- `response_withholding_completed`
- `response_forwarding_failed`

The experiment runner records execution-level measurements and outcomes, including:

- Tool-call elapsed time
- Session elapsed time
- Proxy lifecycle duration
- Injected delay
- Proxy drain duration
- Request and response forwarding status
- Fault activation status
- Protocol outcome
- Semantic outcome
- Trace completeness

The measurements distinguish between client-observed outcomes and proxy-side lifecycle events. In particular, `response_forwarded` indicates that the proxy wrote the response to its client-facing output stream; it does not establish that the client consumed or processed the response.

The logs avoid recording complete prompts and tool arguments, limiting unnecessary data collection.

## 7. Experimental results

The validated experiment consists of three repetitions per condition, for a total of 15 executions. The results below are from `results_submission_v2.jsonl`.

| Condition | Mean tool-call time | Observed outcome |
|---|---:|---|
| Baseline | 0.320 s | Correct result (120), 3/3 |
| Latency | 1.519 s | Correct result (120), delayed, 3/3 |
| Error | 0.009 s | Injected JSON-RPC error, 3/3 |
| Corruption | 0.014 s | Incorrect result (999), 3/3 |
| Hang | 3.006 s | Client timeout, 3/3 |

The baseline mean includes first-run startup overhead: the three measurements were approximately 0.899 s, 0.011 s, and 0.050 s. The baseline median was 0.050 s. This variability is reported rather than excluding the first observation.

The results demonstrate four distinct fault effects:

- **Latency:** increases tool-call duration without changing the returned value.
- **Error:** prevents the upstream tool request from being executed and returns a synthetic error.
- **Corruption:** preserves protocol-level success while changing the semantic content of the result.
- **Hang:** causes the client to time out while the proxy continues and completes its response-withholding operation.

For the hang condition, the client timeout was approximately three seconds, while the proxy withheld the response for approximately five seconds. All three runs recorded completion of the withholding operation and successful proxy-level forwarding. This does not imply that the timed-out client consumed the response.

These measurements describe a controlled demonstrator and should not be interpreted as general performance benchmarks.

## 8. Limitations

This implementation is a minimal MCP-layer fault-injection demonstrator, not a complete implementation of the ReportCrew multi-agent workflow.

In particular:

- It uses a single controlled MCP server and one tool.
- It does not directly integrate with CrewAI, LangGraph, or the actual ReportCrew agents.
- Fault injection targets tool requests and responses; agent-to-agent message corruption is not implemented.
- The latency mechanism introduces a fixed delay rather than a stochastic latency distribution.
- The experiment does not evaluate concurrent tool-call workloads.
- Corruption uses configured text and structured-field modifications rather than a general semantic corruption model.
- The hang experiment withholds a response after the upstream server has responded; it does not simulate an upstream server that never responds.
- The experiment evaluates a controlled tool result, not the correctness of a complete multi-agent report.

The demonstrator therefore establishes the feasibility of MCP-layer fault injection and trace collection. Evaluating end-to-end agent robustness, semantic task success, fault propagation, root-cause localization, and mitigation strategies would require integration with the actual ReportCrew workflow and independent ground-truth evaluation.
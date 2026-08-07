"""Auto-generated Chain Executor for Live Demo — Incident Report Pipeline."""
import asyncio
import json
import os
import time
import uuid

import httpx
from fastapi import FastAPI, Request
from jsonpath_ng import parse as jsonpath_parse

app = FastAPI(title="Live Demo — Incident Report Pipeline")

TOKEN = ""
# h11 (httpx's transport) rejects "Bearer " with nothing after it as an
# illegal header value — only attach Authorization when a token exists.
AGENT_HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
AGENTS = [
    {"id": "node-a7237503", "agent_id": "83730c84-9872-44e5-8d28-0f71bd1eb9f1", "name": "incident-commander", "endpoint": "http://10.42.0.134:9072", "timeout": 30},
    {"id": "node-801fd415", "agent_id": "69d1924f-740d-47a0-9c6c-75db27f1ace6", "name": "report-generator-agent", "endpoint": "http://10.42.0.134:9035", "timeout": 30},
]

# Endpoints baked in above come from the registry at generation time —
# correct when agents run wherever they're registered, stale when this
# orchestrator is deployed alongside its own freshly-spun-up agent pods
# elsewhere (a different cluster/VPC). YARD_AGENT_ENDPOINTS (JSON, node id
# -> endpoint), set by the K8s deploy generator, overrides per-node.
_ENDPOINT_OVERRIDES = json.loads(os.environ.get("YARD_AGENT_ENDPOINTS", "{}"))
for _agent in AGENTS:
    _agent["endpoint"] = _ENDPOINT_OVERRIDES.get(_agent["id"], _agent["endpoint"])

# Incoming-edge transform per target node id — {"strategy": ..., "mappings": {...}}.
# Only the *previous* node's outgoing edge matters for a sequential chain
# (each node has at most one incoming edge here), so this is keyed by target.
# Each field is rendered as its own JSON string (double `tojson`) rather
# than inline dict/list syntax — a bare `tojson` emits real JSON
# (null/true/false), which isn't valid Python; json.loads() per field turns
# it back into a real value. Only strategy + mappings are carried through —
# the only two apply_transform() below actually reads.
EDGE_TRANSFORM_BY_TARGET = {
    "node-801fd415": {
        "strategy": "passthrough",
        "mappings": json.loads("null"),
    },
}


def apply_transform(transform: dict, source_output: dict) -> dict:
    """Mirrors the platform's own transforms/*.py strategies, self-contained
    since this generated container doesn't import the platform package.
    Covers passthrough + explicit_mapping (what's actually exercised by
    generated systems today); auto_negotiate/llm_transform/
    supervisor_handles fall back to passthrough rather than silently
    dropping fields — a real gap, but a safer default than guessing.
    """
    strategy = (transform or {}).get("strategy", "passthrough")
    if strategy == "explicit_mapping":
        mappings = (transform or {}).get("mappings") or {}
        if not mappings:
            return source_output
        result = {}
        for target_key, path_expr in mappings.items():
            try:
                matches = jsonpath_parse(path_expr).find(source_output)
                if matches:
                    result[target_key] = matches[0].value
            except Exception:
                pass
        return result
    return source_output


async def call_agent(endpoint: str, input_data: dict, invocation_id: str, trace_id: str) -> dict:
    headers = {**AGENT_HEADERS, "X-Invocation-ID": invocation_id, "X-Trace-ID": trace_id}
    async with httpx.AsyncClient() as client:
        resp = await client.post(endpoint, json={"input": input_data}, headers=headers, timeout=300)
        resp.raise_for_status()
        return resp.json()


@app.post("/invoke")
async def invoke(input: dict, request: Request):
    current = input.get("input", input)
    # Propagate whatever the caller sent (Mission Control's engine forwards
    # the real invocation id here) so this system's trace correlates with
    # the invocation shown in the UI; generate one when called directly
    # (e.g. curl) so every invoke still gets a real, connected Jaeger trace
    # across all its nodes instead of each agent minting its own.
    invocation_id = request.headers.get("X-Invocation-ID") or uuid.uuid4().hex
    trace_id = request.headers.get("X-Trace-ID") or invocation_id
    trace = []
    for i, agent in enumerate(AGENTS):
        transform = EDGE_TRANSFORM_BY_TARGET.get(agent["id"])
        if transform is not None:
            current = apply_transform(transform, current)
        start = time.monotonic()
        try:
            result = await call_agent(agent["endpoint"], current, invocation_id, trace_id)
            duration_ms = int((time.monotonic() - start) * 1000)
            # Mission Control's trace step renderer keys rows by `step` and
            # reads `agent_name`/`duration_ms` (matches the local
            # chain_executor's own trace shape) — this used to emit `agent`
            # with no `step` or timing at all, so every deployed-
            # orchestrator invoke rendered blank step names, "(ms)" with no
            # number, and React key warnings for the undefined `step`.
            trace.append({"step": i + 1, "agent_name": agent["name"], "status": "completed", "input": current, "output": result, "duration_ms": duration_ms})
            current = result.get("output", result)
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            # A sequential chain has no business calling step i+2 when step
            # i+1 just failed — every later node's input is downstream of
            # the value that never got produced, so it would only fail
            # again (or silently run on stale data). Matches the local
            # (non-deployed) chain_executor's own on_failure="return_partial"
            # contract: stop here, report exactly how far we got.
            trace.append({"step": i + 1, "agent_name": agent["name"], "status": "failed_partial", "input": current, "error": str(e), "duration_ms": duration_ms})
            return {
                "output": current, "trace": trace, "status": "partial",
                "error": f"Agent {agent['name']} failed at step {i + 1}: {e}",
                "partial": True, "completed_steps": i, "total_steps": len(AGENTS),
            }
    return {"output": current, "trace": trace, "status": "completed", "error": None}


@app.get("/health")
async def health():
    return {"status": "ok", "engine": "chain_executor", "agents": len(AGENTS)}
---
name: pipeline-workflows
description: Drive a workflow run through the workflow_* tools of a CAVI pipeline MCP server. Use when a pipeline server lists workflow_list, workflow_start or workflow_status and the task is to run, resume, retry, approve or cancel one of its graphs.
---

# Pipeline workflows

A pipeline server that mounts the engine exposes one graph per workflow. A run of a graph lives in one run directory, and every node writes its declared outputs there.

## Node kinds

- `agent`: you do the work. `workflow_begin`, write the node's outputs, then `workflow_record`. When the server's own tools wrote the outputs, those tools record the node; read `workflow_status` after them.
- `step`: the engine runs it as soon as it is ready. You only see the result.
- `approval`: a person decides. Ask them, then `workflow_approve` with `approve` or `reject` and their reason.

## Loop

1. `workflow_list` names the graphs and the runs already in the store. Resume a live run with `workflow_status` instead of starting another; a run directory holds one live run.
2. `workflow_start` with the graph and the run directory, relative to the runs workspace.
3. Read `ready` and `tasks`. Each task gives the node's inputs, outputs and instructions.
4. For an agent node: `workflow_begin`, do the task, write every listed output, `workflow_record`. A refused record names what to fix and leaves the node running; fix it and record again.
5. Repeat from the returned status until `complete` is true.

Every write takes the `revision` from the latest status. A refused revision means the run moved; read `workflow_status` and act on what it shows.

## Failures and changes

- Work that cannot finish: `workflow_fail` with the reason. When you cannot tell whether it had an effect (a timeout, a lost connection), pass `uncertain: true`; the node keeps its resources until someone checks and calls `workflow_retry`.
- A step that fails shows its reason; fix the cause, then `workflow_retry` it.
- A node whose outputs or inputs changed after it was recorded shows as `stale`, and so does everything that depends on it. The next write returns them to waiting; do them again in order.
- `blocked` on a ready node names the run holding a resource it needs. It starts on the next write once the resource is free; `workflow_retry` starts a blocked step again.
- `workflow_cancel` ends a run and releases its resources. A cancelled run takes no writes.

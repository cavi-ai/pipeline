"""Evidence of what a finished node consumed and produced, and whether it still holds."""

import hashlib
import json
import os

from cavi_mcp_core.files import digest, inside


def _fingerprint(run_dir, relative):
    path = inside(run_dir, relative, strict=True)
    try:
        info = path.stat()
        sha256 = digest(path, limit=None)
    except FileNotFoundError:
        return None
    except ValueError as exc:
        raise ValueError(f"{relative}: {exc}") from None
    return {"sha256": sha256, "size": info.st_size, "mtime_ns": info.st_mtime_ns}


def _matches(run_dir, relative, recorded):
    try:
        info = os.stat(inside(run_dir, relative, strict=True))
    except (FileNotFoundError, ValueError):
        return False
    if (info.st_size, info.st_mtime_ns) == (recorded["size"], recorded["mtime_ns"]):
        return True
    current = _fingerprint(run_dir, relative)
    return current is not None and current["sha256"] == recorded["sha256"]


def evidence(node, run_dir, upstream: dict[str, str]) -> dict:
    """Fingerprints of the node's inputs and outputs, with the receipts of its dependencies.

    A missing declared input or output refuses the node.
    """
    files = {}
    for role, paths in (("input", node.inputs), ("output", node.output_paths)):
        for relative in paths:
            found = _fingerprint(run_dir, relative)
            if found is None:
                raise ValueError(f"node {node.id} {role} {relative} does not exist")
            files[relative] = found
    return {"files": files, "upstream": dict(upstream)}


def receipt(recorded: dict) -> str:
    """The digest a dependent node keeps of this node's evidence."""
    canonical = {
        "files": {path: entry["sha256"] for path, entry in sorted(recorded["files"].items())},
        "upstream": dict(sorted(recorded["upstream"].items())),
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def stale(graph, nodes: dict, run_dir) -> dict[str, str]:
    """Done nodes whose evidence no longer holds, each with the reason."""
    fresh, found = set(), {}
    for node_id in graph.order():
        entry = nodes[node_id]
        if entry["status"] != "done":
            continue
        node = graph.nodes[node_id]
        recorded = entry["evidence"]
        reason = None
        for dependency in node.depends:
            if dependency not in fresh:
                reason = f"{dependency} is no longer done"
            elif recorded["upstream"].get(dependency) != nodes[dependency]["receipt"]:
                reason = f"{dependency} was recorded again"
            if reason:
                break
        if reason is None:
            for relative, fingerprint in recorded["files"].items():
                if not _matches(run_dir, relative, fingerprint):
                    reason = f"{relative} changed"
                    break
        if reason is None:
            fresh.add(node_id)
        else:
            found[node_id] = reason
    return found

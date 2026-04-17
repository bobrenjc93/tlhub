from __future__ import annotations

from collections import defaultdict
import json
import re
from typing import Any


PROVENANCE_MAPPING_FAMILY = "artifact:inductor_provenance_tracking_node_mappings"
PROVENANCE_PRE_FAMILIES = (
    "artifact:before_pre_grad_graph",
    "artifact:inductor_pre_grad_graph",
)
PROVENANCE_POST_FAMILIES = (
    "artifact:after_post_grad_graph",
    "artifact:inductor_post_grad_graph",
)
PROVENANCE_AOT_CODE_FAMILIES = (
    "graph_dump:inductor_aot_wrapper_code",
    "graph_dump:inductor_aot_kernel_code",
)
PROVENANCE_EXTRA_FAMILIES = (
    "artifact:inductor_post_to_pre_grad_nodes",
    "artifact:inductor_triton_kernel_to_post_grad_nodes",
    "graph_dump:inductor_aot_kernel_code",
)

_CPP_XNUMEL_RE = re.compile(r"^\s*\w+\s+.*_xnumel(?:_\d+)?\s*=")
_GRAPH_ASSIGN_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_.]*)\s*:\s*.*?=\s*(?P<expr>.+?)(?:;\s*.*)?$"
)
_GRAPH_RETURN_RE = re.compile(r"^\s*return\s*(?P<expr>.+)$")
_JSON_KEY_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_./-]*)")


def base_family(family: str) -> str:
    if family.startswith("rank:") and "/" in family:
        return family.split("/", 1)[1]
    return family


def build_provenance_groups(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = [
        {**artifact, "_base_family": base_family(str(artifact.get("family") or ""))}
        for artifact in sorted(artifacts, key=_artifact_sort_key)
    ]
    mapping_artifacts = [
        artifact for artifact in ordered if artifact["_base_family"] == PROVENANCE_MAPPING_FAMILY
    ]
    if not mapping_artifacts:
        return []

    by_scope: dict[tuple[Any, Any] | None, list[dict[str, Any]]] = defaultdict(list)
    for artifact in ordered:
        by_scope[_artifact_scope(artifact)].append(artifact)

    groups: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    for mapping in mapping_artifacts:
        scope = _artifact_scope(mapping)
        bucket = by_scope.get(scope, [])
        pre_grad = _select_nearest_artifact(
            mapping,
            bucket,
            PROVENANCE_PRE_FAMILIES,
            used_ids,
        )
        post_grad = _select_nearest_artifact(
            mapping,
            bucket,
            PROVENANCE_POST_FAMILIES,
            used_ids,
        )
        output_code = _select_nearest_artifact_prefix(
            mapping,
            bucket,
            "inductor_output_code:",
            used_ids,
        )
        aot_code = _select_nearest_artifact(
            mapping,
            bucket,
            PROVENANCE_AOT_CODE_FAMILIES,
            used_ids,
        )
        extra_ids = [
            artifact["id"]
            for artifact in _select_extra_artifacts(mapping, bucket, used_ids)
        ]

        selected_ids = [mapping["id"]]
        for artifact in (pre_grad, post_grad, output_code, aot_code):
            if artifact is not None:
                selected_ids.append(artifact["id"])
        selected_ids.extend(extra_ids)
        used_ids.update(selected_ids)

        label = (
            str(mapping.get("compile_id") or "").strip()
            or str(mapping.get("compile_dir") or "").strip()
            or f"Provenance {len(groups) + 1}"
        )
        groups.append(
            {
                "id": f"prov-{len(groups) + 1:04d}",
                "label": label,
                "rank": mapping.get("rank"),
                "compile_id": mapping.get("compile_id"),
                "compile_dir": mapping.get("compile_dir"),
                "mapping_artifact_id": mapping["id"],
                "pre_grad_artifact_id": pre_grad["id"] if pre_grad else None,
                "post_grad_artifact_id": post_grad["id"] if post_grad else None,
                "output_code_artifact_id": output_code["id"] if output_code else None,
                "aot_code_artifact_id": aot_code["id"] if aot_code else None,
                "extra_artifact_ids": extra_ids,
            }
        )
    return groups


def build_provenance_line_mappings(
    node_mappings: dict[str, Any] | None,
    pre_grad_graph_content: str,
    post_grad_graph_content: str,
    output_code_content: str,
    aot_code_content: str,
) -> dict[str, dict[str, list[int]]]:
    node_mappings = node_mappings or {}
    if not isinstance(node_mappings, dict):
        return _empty_line_mappings()

    raw_cpp_code_to_post = _json_object(node_mappings.get("cppCodeToPost"))
    raw_post_to_cpp_code = _json_object(node_mappings.get("postToCppCode"))
    raw_py_code_to_post = (
        _json_object(node_mappings.get("pyCodeToPost"))
        or _json_object(node_mappings.get("codeToPost"))
        or raw_cpp_code_to_post
    )
    raw_post_to_py_code = (
        _json_object(node_mappings.get("postToPyCode"))
        or _json_object(node_mappings.get("postToCode"))
        or raw_post_to_cpp_code
    )

    kernel_names = sorted(
        {
            *raw_cpp_code_to_post.keys(),
            *raw_post_to_cpp_code.keys(),
            *raw_py_code_to_post.keys(),
            *raw_post_to_py_code.keys(),
        }
    )

    pre_grad_node_to_lines = _build_node_to_lines_map(pre_grad_graph_content)
    post_grad_node_to_lines = _build_node_to_lines_map(post_grad_graph_content)
    py_kernel_to_lines = _build_python_kernel_to_lines_map(output_code_content, kernel_names)
    cpp_kernel_to_lines = _build_cpp_kernel_to_lines_map(aot_code_content, kernel_names)

    return {
        "preToPost": _process_node_mappings(
            _json_object(node_mappings.get("preToPost")),
            pre_grad_node_to_lines,
            post_grad_node_to_lines,
        ),
        "postToPre": _process_node_mappings(
            _json_object(node_mappings.get("postToPre")),
            post_grad_node_to_lines,
            pre_grad_node_to_lines,
        ),
        "pyCodeToPost": _process_kernel_to_post_mappings(
            raw_py_code_to_post,
            py_kernel_to_lines,
            post_grad_node_to_lines,
        ),
        "postToPyCode": _process_post_to_kernel_mappings(
            raw_post_to_py_code,
            post_grad_node_to_lines,
            py_kernel_to_lines,
        ),
        "cppCodeToPost": _process_kernel_to_post_mappings(
            raw_cpp_code_to_post,
            cpp_kernel_to_lines,
            post_grad_node_to_lines,
        ),
        "postToCppCode": _process_post_to_kernel_mappings(
            raw_post_to_cpp_code,
            post_grad_node_to_lines,
            cpp_kernel_to_lines,
        ),
    }


def build_fx_graph_diff(left_text: str, right_text: str) -> dict[str, Any] | None:
    left_nodes = _parse_fx_graph_nodes(left_text)
    right_nodes = _parse_fx_graph_nodes(right_text)
    if not left_nodes and not right_nodes:
        return None

    left_by_name = {node["name"]: node for node in left_nodes}
    right_by_name = {node["name"]: node for node in right_nodes}
    left_names = {node["name"] for node in left_nodes}
    right_names = {node["name"] for node in right_nodes}

    removed = [left_by_name[name] for name in sorted(left_names - right_names)]
    added = [right_by_name[name] for name in sorted(right_names - left_names)]
    changed = [
        {
            "name": name,
            "left": left_by_name[name],
            "right": right_by_name[name],
        }
        for name in sorted(left_names & right_names)
        if left_by_name[name]["signature"] != right_by_name[name]["signature"]
    ]

    left_order = [node["name"] for node in left_nodes if node["name"] in right_names]
    right_order = [node["name"] for node in right_nodes if node["name"] in left_names]
    return {
        "left_count": len(left_nodes),
        "right_count": len(right_nodes),
        "added": added,
        "removed": removed,
        "changed": changed,
        "order_changed": left_order != right_order,
        "left_nodes": left_nodes,
        "right_nodes": right_nodes,
    }


def build_json_diff(left_text: str, right_text: str) -> dict[str, Any] | None:
    try:
        left = json.loads(left_text)
        right = json.loads(right_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(left, dict) or not isinstance(right, dict):
        return None

    left_keys = set(left)
    right_keys = set(right)
    changed = [
        key for key in sorted(left_keys & right_keys) if json.dumps(left[key], sort_keys=True) != json.dumps(right[key], sort_keys=True)
    ]
    return {
        "added": sorted(right_keys - left_keys),
        "removed": sorted(left_keys - right_keys),
        "changed": changed,
    }


def _artifact_sort_key(artifact: dict[str, Any]) -> tuple[Any, ...]:
    return (
        artifact.get("rank") if artifact.get("rank") is not None else -1,
        str(artifact.get("log_file") or ""),
        int(artifact.get("line_no") or 0),
        int(artifact.get("family_index") or 0),
        str(artifact.get("relative_path") or ""),
    )


def _artifact_scope(artifact: dict[str, Any]) -> tuple[Any, Any] | None:
    compile_key = artifact.get("compile_dir") or artifact.get("compile_id")
    if compile_key is None and artifact.get("rank") is None:
        return None
    return (artifact.get("rank"), compile_key)


def _select_nearest_artifact(
    anchor: dict[str, Any],
    artifacts: list[dict[str, Any]],
    families: tuple[str, ...],
    used_ids: set[str],
) -> dict[str, Any] | None:
    matches = [
        artifact
        for artifact in artifacts
        if artifact["id"] not in used_ids and artifact["_base_family"] in families
    ]
    if not matches:
        return None
    return min(matches, key=lambda artifact: _distance(anchor, artifact))


def _select_nearest_artifact_prefix(
    anchor: dict[str, Any],
    artifacts: list[dict[str, Any]],
    family_prefix: str,
    used_ids: set[str],
) -> dict[str, Any] | None:
    matches = [
        artifact
        for artifact in artifacts
        if artifact["id"] not in used_ids and artifact["_base_family"].startswith(family_prefix)
    ]
    if not matches:
        return None
    return min(matches, key=lambda artifact: _distance(anchor, artifact))


def _select_extra_artifacts(
    anchor: dict[str, Any],
    artifacts: list[dict[str, Any]],
    used_ids: set[str],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    local_used = set(used_ids)
    for family in PROVENANCE_EXTRA_FAMILIES:
        artifact = _select_nearest_artifact(anchor, artifacts, (family,), local_used)
        if artifact is None:
            continue
        local_used.add(artifact["id"])
        results.append(artifact)
    return results


def _distance(anchor: dict[str, Any], artifact: dict[str, Any]) -> tuple[int, int]:
    line_distance = abs(int(anchor.get("line_no") or 0) - int(artifact.get("line_no") or 0))
    path_mismatch = int(str(anchor.get("log_file") or "") != str(artifact.get("log_file") or ""))
    return (path_mismatch, line_distance)


def _empty_line_mappings() -> dict[str, dict[str, list[int]]]:
    return {
        "preToPost": {},
        "postToPre": {},
        "pyCodeToPost": {},
        "postToPyCode": {},
        "cppCodeToPost": {},
        "postToCppCode": {},
    }


def _json_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _build_node_to_lines_map(content: str) -> dict[str, int]:
    node_to_lines: dict[str, int] = {}
    for line_number, line in enumerate(content.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        before_equals = stripped.split("=", 1)[0]
        node_name = before_equals.split(":", 1)[0].strip()
        if node_name:
            node_to_lines[node_name] = line_number
    return node_to_lines


def _build_python_kernel_to_lines_map(
    content: str,
    kernel_names: list[str],
) -> dict[str, list[int]]:
    content = "\n".join(line for line in content.splitlines() if line or content)
    lines = content.splitlines()
    kernel_to_lines: dict[str, list[int]] = defaultdict(list)
    if not lines:
        return {}

    run_impl_line = next(
        (
            index
            for index, line in enumerate(lines)
            if "def" in line and "call" in line and "(args)" in line
        ),
        0,
    )
    first_line_number = next((index for index, line in enumerate(lines) if "# AOT ID:" in line), 0)

    for kernel_name in kernel_names:
        pure_kernel_name = kernel_name.split(":", 1)[0]
        found = False
        if ":" in kernel_name:
            for index, line in enumerate(lines[run_impl_line:], start=run_impl_line):
                if kernel_name in line:
                    for probe_index, probe_line in enumerate(lines[index + 1 :], start=index + 1):
                        if pure_kernel_name in probe_line:
                            kernel_to_lines[kernel_name].append(
                                probe_index + 1 - first_line_number
                            )
                            found = True
                            break
                    break
        if found:
            continue
        for index, line in enumerate(lines[run_impl_line:], start=run_impl_line):
            if pure_kernel_name in line:
                kernel_to_lines[kernel_name].append(index + 1 - first_line_number)
    return dict(kernel_to_lines)


def _build_cpp_kernel_to_lines_map(
    content: str,
    kernel_names: list[str],
) -> dict[str, list[int]]:
    lines = [line for line in content.splitlines() if line or content]
    kernel_to_lines: dict[str, list[int]] = defaultdict(list)
    if not lines:
        return {}

    run_impl_line = next(
        (index for index, line in enumerate(lines) if "::run_impl(" in line),
        0,
    )
    for kernel_name in kernel_names:
        pure_kernel_name = kernel_name.rsplit(":", 1)[0]
        found = False
        if ":" in kernel_name:
            for index, line in enumerate(lines[run_impl_line:], start=run_impl_line):
                if (
                    "def" not in line
                    and "static inline void" not in line
                    and kernel_name in line
                ):
                    continue
                if kernel_name not in line:
                    continue
                for probe_index, probe_line in enumerate(lines[index + 1 :], start=index + 1):
                    if pure_kernel_name not in probe_line:
                        continue
                    if "_xnumel = " in probe_line or _CPP_XNUMEL_RE.match(probe_line):
                        continue
                    kernel_to_lines[kernel_name].append(probe_index + 1)
                    found = True
                    break
                if found:
                    break
        if found:
            continue
        for index, line in enumerate(lines[run_impl_line:], start=run_impl_line):
            if pure_kernel_name in line:
                kernel_to_lines[kernel_name].append(index + 1)
    return dict(kernel_to_lines)


def _process_node_mappings(
    source_mappings: dict[str, Any],
    source_lookup: dict[str, int],
    target_lookup: dict[str, int],
) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for source_node, target_nodes in source_mappings.items():
        source_line = source_lookup.get(source_node)
        if source_line is None:
            continue
        target_lines = [
            target_lookup[target_node]
            for target_node in _coerce_string_list(target_nodes)
            if target_node in target_lookup
        ]
        if target_lines:
            result[str(source_line)] = target_lines
    return result


def _process_kernel_to_post_mappings(
    kernel_mappings: dict[str, Any],
    kernel_lookup: dict[str, list[int]],
    post_lookup: dict[str, int],
) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for kernel_name, post_nodes in kernel_mappings.items():
        kernel_lines = kernel_lookup.get(kernel_name, [])
        if not kernel_lines:
            continue
        target_lines = [
            post_lookup[post_node]
            for post_node in _coerce_string_list(post_nodes)
            if post_node in post_lookup
        ]
        if not target_lines:
            continue
        for kernel_line in kernel_lines:
            result[str(kernel_line)] = target_lines
    return result


def _process_post_to_kernel_mappings(
    post_mappings: dict[str, Any],
    post_lookup: dict[str, int],
    kernel_lookup: dict[str, list[int]],
) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for post_node, kernel_names in post_mappings.items():
        post_line = post_lookup.get(post_node)
        if post_line is None:
            continue
        target_lines: list[int] = []
        for kernel_name in _coerce_string_list(kernel_names):
            target_lines.extend(kernel_lookup.get(kernel_name, []))
        if target_lines:
            result[str(post_line)] = sorted(set(target_lines))
    return result


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if value is None:
        return []
    return [str(value)]


def _parse_fx_graph_nodes(content: str) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        assign_match = _GRAPH_ASSIGN_RE.match(line)
        if assign_match:
            name = assign_match.group("name")
            expr = assign_match.group("expr").strip()
            target = expr.split("(", 1)[0].strip()
            nodes.append(
                {
                    "line": line_number,
                    "name": name,
                    "expr": expr,
                    "target": target,
                    "signature": f"{name}={target}",
                }
            )
            continue
        return_match = _GRAPH_RETURN_RE.match(line)
        if return_match:
            expr = return_match.group("expr").strip()
            nodes.append(
                {
                    "line": line_number,
                    "name": "return",
                    "expr": expr,
                    "target": "return",
                    "signature": f"return={expr}",
                }
            )
    return nodes

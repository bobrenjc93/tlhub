from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterator, TextIO
import uuid

from tlhub.database import ArtifactCreate


GLOG_RE = re.compile(
    r"(?P<level>[VIWEC])(?P<month>\d{2})(?P<day>\d{2}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\.(?P<microsecond>\d{6}) "
    r"(?P<thread>\d+) (?P<pathname>[^:]+):(?P<source_line>\d+)\] (?P<payload>.*)"
)

GRAPH_EVENTS = {
    "dynamo_output_graph",
    "optimize_ddp_split_graph",
    "compiled_autograd_graph",
    "aot_forward_graph",
    "aot_backward_graph",
    "aot_inference_graph",
    "aot_joint_graph",
    "inductor_pre_grad_graph",
    "inductor_post_grad_graph",
}

SKIP_EVENTS = {"chromium_event"}

STANDARD_KEYS = {
    "rank",
    "compiled_autograd_id",
    "frame_id",
    "frame_compile_id",
    "attempt",
    "has_payload",
    "stack",
}

COLLECTIVE_CALL_RE = re.compile(
    r"torch\s*\.\s*ops\s*\.\s*_?c10d_functional\s*\.\s*([A-Za-z0-9_]+)\s*\.\s*default\s*\("
)
COMMENT_RE = re.compile(r"#.*$|//.*$|/\*.*?\*/", re.MULTILINE | re.DOTALL)
HTML_TAG_RE = re.compile(r"(?s)<[^>]*>")
SEED_NSPID_RE = re.compile(r"[^/]+-seed-nspid[^/]+/")

CONVERT_FRAME_SUFFIXES = (
    (
        ("torch/_dynamo/convert_frame.py", "catch_errors"),
        ("torch/_dynamo/convert_frame.py", "_convert_frame"),
        ("torch/_dynamo/convert_frame.py", "_convert_frame_assert"),
    ),
    (
        ("torch/_dynamo/convert_frame.py", "__call__"),
        ("torch/_dynamo/convert_frame.py", "__call__"),
        ("torch/_dynamo/convert_frame.py", "__call__"),
    ),
)


@dataclass(frozen=True)
class ParsedEvent:
    event_type: str
    envelope: dict[str, Any]
    payload: str
    log_line_no: int
    source_line_no: int
    log_file: str
    rank: int | None
    compile_id: str | None
    compile_dir: str | None
    timestamp: str
    thread: int | str
    pathname: str


@dataclass(frozen=True)
class ParseResult:
    artifacts: list[ArtifactCreate]
    log_files: list[str]
    warnings: list[str]
    manifest: dict[str, Any]


@dataclass(frozen=True)
class ArtifactPlan:
    family: str
    kind: str
    title: str
    subpath: Path
    content: str
    encoding: str
    content_type: str


class TraceIngestor:
    def __init__(self, run_id: str, trace_dir: Path, artifacts_root: Path) -> None:
        self.run_id = run_id
        self.trace_dir = trace_dir
        self.artifacts_root = artifacts_root
        self.run_root = artifacts_root.parent
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        self.run_root.mkdir(parents=True, exist_ok=True)

        self.family_counts: Counter[str] = Counter()
        self.used_paths: set[str] = set()
        self.artifacts: list[ArtifactCreate] = []
        self.artifact_number = 0
        self.warnings: list[str] = []
        self.log_files: list[str] = []
        self.string_table: dict[int, str] = {}
        self.raw_rows: list[str] = []
        self.compiles: dict[str, dict[str, Any]] = {}
        self.failures_and_restarts: list[dict[str, Any]] = []
        self.export_failures: list[dict[str, Any]] = []
        self.guard_details: list[dict[str, Any]] = []
        self.sym_expr_info: dict[int, dict[str, Any]] = {}
        self.unknown_stacks: list[list[dict[str, Any]]] = []
        self.runtime_estimations: list[dict[str, Any]] = []
        self.collective_schedules: list[dict[str, Any]] = []
        self.tensor_meta_signatures: list[dict[str, Any]] = []
        self.chromium_events: list[dict[str, Any]] = []
        self.exec_orders: dict[int, list[str]] = {}
        self.exported_program_artifact_id: str | None = None
        self.report_artifacts: list[dict[str, Any]] = []
        self.vllm_state: dict[str, Any] = {
            "config": None,
            "piecewise_graph": None,
            "subgraphs": [],
            "has_artifacts": False,
            "current_subgraph": None,
        }

    def ingest(self) -> ParseResult:
        trace_files = discover_trace_files(self.trace_dir)
        self.log_files = [path.relative_to(self.trace_dir).as_posix() for path in trace_files]

        for trace_file in trace_files:
            relative_log = trace_file.relative_to(self.trace_dir).as_posix()
            for event in iter_trace_events(
                trace_file,
                relative_log,
                self.warnings,
                self.string_table,
            ):
                self.process_event(event)

        self.finalize_reports()
        manifest = self.build_manifest()
        (self.run_root / "run_manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return ParseResult(
            artifacts=self.artifacts,
            log_files=self.log_files,
            warnings=self.warnings,
            manifest=manifest,
        )

    def process_event(self, event: ParsedEvent) -> None:
        compile_entry = self.get_compile_entry(event)
        payload_path: str | None = None
        artifact_descriptor: dict[str, Any] | None = None

        if event.event_type == "expression_created":
            info = event.envelope.get("expression_created") or {}
            result_id = info.get("result_id")
            if isinstance(result_id, int):
                self.sym_expr_info[result_id] = normalize_jsonable(info)

        if event.envelope.get("stack") and event.compile_id is None:
            decoded = decode_stack(event.envelope.get("stack"), self.string_table)
            if decoded:
                self.unknown_stacks.append(decoded)

        self.record_event_metadata(event, compile_entry)

        if event.event_type == "chromium_event":
            self.record_chromium_event(event)

        plan = build_artifact_plan(event)
        if plan is not None:
            artifact, descriptor = self.write_artifact(
                family=plan.family,
                kind=plan.kind,
                title=plan.title,
                content=plan.content,
                candidate_path=self.build_event_relative_path(event, plan),
                encoding=plan.encoding,
                content_type=plan.content_type,
                event_type=event.event_type,
                rank=event.rank,
                compile_id=event.compile_id,
                compile_dir=event.compile_dir,
                log_file=event.log_file,
                line_no=event.log_line_no,
            )
            payload_path = artifact.relative_path
            artifact_descriptor = descriptor
            if compile_entry is not None:
                compile_entry["artifacts"].append(descriptor)
                compile_entry["artifact_count"] = len(compile_entry["artifacts"])
                compile_entry["last_line"] = max(compile_entry["last_line"], event.log_line_no)
            self.record_post_artifact_metadata(event, artifact_descriptor, compile_entry)

        if event.event_type not in SKIP_EVENTS:
            self.record_raw_row(event, payload_path)

    def record_event_metadata(
        self,
        event: ParsedEvent,
        compile_entry: dict[str, Any] | None,
    ) -> None:
        envelope = event.envelope
        event_type = event.event_type

        if compile_entry is not None:
            compile_entry["event_types"].add(event_type)

        if event_type == "dynamo_start":
            metadata = envelope.get("dynamo_start") or {}
            stack = decode_stack(metadata.get("stack"), self.string_table)
            stack = maybe_remove_convert_frame_suffixes(stack)
            if stack:
                if compile_entry is not None and not compile_entry["stack"]:
                    compile_entry["stack"] = stack
                elif compile_entry is None:
                    self.unknown_stacks.append(stack)
            return

        if event_type == "symbolic_shape_specialization" and compile_entry is not None:
            compile_entry["symbolic_shape_specializations"].append(
                normalize_symbolic_shape_specialization(
                    envelope.get("symbolic_shape_specialization") or {},
                    self.string_table,
                )
            )
            return

        if event_type == "guard_added_fast" and compile_entry is not None:
            compile_entry["guards_added_fast"].append(
                normalize_guard_added_fast(
                    envelope.get("guard_added_fast") or {},
                    self.string_table,
                )
            )
            return

        if event_type == "create_symbol" and compile_entry is not None:
            compile_entry["create_symbols"].append(
                normalize_symbol_record(
                    envelope.get("create_symbol") or {},
                    self.string_table,
                )
            )
            return

        if event_type == "create_unbacked_symbol" and compile_entry is not None:
            compile_entry["unbacked_symbols"].append(
                normalize_unbacked_symbol_record(
                    envelope.get("create_unbacked_symbol") or {},
                    self.string_table,
                )
            )
            return

        if event_type == "link" and compile_entry is not None:
            metadata = envelope.get("link") or {}
            compile_entry["links"].append(
                {
                    "name": str(metadata.get("name") or ""),
                    "url": str(metadata.get("url") or ""),
                }
            )
            return

        if event_type == "compilation_metrics" and compile_entry is not None:
            metrics = normalize_jsonable(envelope.get("compilation_metrics") or {})
            compile_entry["compilation_metrics"] = metrics
            compile_entry["status"] = compute_compile_status(metrics)
            self.record_failures_and_restarts(compile_entry, metrics)
            return

        if event_type == "bwd_compilation_metrics" and compile_entry is not None:
            compile_entry["bwd_compilation_metrics"] = normalize_jsonable(
                envelope.get("bwd_compilation_metrics") or {}
            )
            return

        if event_type == "aot_autograd_backward_compilation_metrics" and compile_entry is not None:
            compile_entry["aot_autograd_backward_compilation_metrics"] = normalize_jsonable(
                envelope.get("aot_autograd_backward_compilation_metrics") or {}
            )
            return

        if event_type == "guard_added":
            metadata = envelope.get("guard_added") or {}
            if metadata.get("prefix") == "eval":
                self.record_export_guard_failure(
                    compile_entry,
                    event,
                    failure_type="Guard Evaluated",
                    reason=(
                        "When exporting, this guard was evaluated and may have produced "
                        "a constraint violation."
                    ),
                )
            return

        if event_type == "propagate_real_tensors_provenance":
            self.record_export_guard_failure(
                compile_entry,
                event,
                failure_type="Data Dependent Error",
                reason=(
                    "Export specialized a data-dependent symbolic expression and inserted "
                    "asserts into the graph."
                ),
            )
            return

        if event_type == "missing_fake_kernel":
            metadata = envelope.get("missing_fake_kernel") or {}
            self.export_failures.append(
                {
                    "failure_type": "Missing Fake Kernel",
                    "reason": f"torch.ops.{metadata.get('op') or '(unknown)'} is missing a fake kernel implementation",
                    "detail_id": None,
                    "compile_id": event.compile_id,
                    "compile_dir": event.compile_dir,
                    "rank": event.rank,
                }
            )
            return

        if event_type == "mismatched_fake_kernel":
            metadata = envelope.get("mismatched_fake_kernel") or {}
            self.export_failures.append(
                {
                    "failure_type": "Mismatched Fake Kernel",
                    "reason": (
                        f"torch.ops.{metadata.get('op') or '(unknown)'} fake kernel mismatch: "
                        f"{metadata.get('reason') or 'unknown reason'}"
                    ),
                    "detail_id": None,
                    "compile_id": event.compile_id,
                    "compile_dir": event.compile_dir,
                    "rank": event.rank,
                }
            )
            return

        if event_type == "artifact":
            metadata = envelope.get("artifact") or {}
            name = str(metadata.get("name") or "")
            if compile_entry is not None and name:
                update_cache_status(compile_entry, name)
            if name == "vllm_piecewise_compile_start":
                self.record_vllm_subgraph(event.payload)
            elif name == "vllm_compilation_config":
                self.record_vllm_config(event.payload)
            return

    def record_post_artifact_metadata(
        self,
        event: ParsedEvent,
        artifact: dict[str, Any],
        compile_entry: dict[str, Any] | None,
    ) -> None:
        envelope = event.envelope
        event_type = event.event_type

        if event_type == "exported_program":
            self.exported_program_artifact_id = artifact["id"]

        if event_type == "artifact":
            metadata = envelope.get("artifact") or {}
            name = str(metadata.get("name") or "")
            if name == "inductor_runtime_and_tensor_meta":
                self.record_runtime_and_tensor_meta(event, artifact, compile_entry)
            elif name == "inductor_collective_schedule":
                self.record_collective_schedule(event, artifact, compile_entry)
            elif "graph_execution" in name and artifact["kind"] == "json":
                self.record_execution_order(event, artifact)
            elif name.startswith("vllm_"):
                self.record_vllm_artifact(name, artifact)
            return

        if event_type == "inductor_output_code" and compile_entry is not None:
            compile_entry["inductor_output_code_collectives"] = extract_collective_calls(
                read_artifact_content(self.artifacts_root, artifact["relative_path"])
            )
            compile_entry["inductor_output_code_artifact_id"] = artifact["id"]
            return

        if event_type == "graph_dump":
            metadata = envelope.get("graph_dump") or {}
            name = str(metadata.get("name") or "")
            if name == "vllm_piecewise_split_graph":
                self.vllm_state["piecewise_graph"] = artifact
                self.vllm_state["has_artifacts"] = True
            elif name.startswith("vllm_subgraph_") or name.startswith("vllm_submod_"):
                self.record_vllm_artifact(name, artifact)
            return

    def record_failures_and_restarts(
        self,
        compile_entry: dict[str, Any],
        metrics: dict[str, Any],
    ) -> None:
        restart_reasons = metrics.get("restart_reasons") or []
        for reason in restart_reasons:
            self.failures_and_restarts.append(
                {
                    "kind": "restart",
                    "compile_id": compile_entry["compile_id"],
                    "compile_dir": compile_entry["compile_dir"],
                    "rank": compile_entry["rank"],
                    "failure_type": "RestartAnalysis",
                    "reason": str(reason),
                    "source": None,
                }
            )
        if metrics.get("fail_type"):
            source = None
            filename = metrics.get("fail_user_frame_filename")
            lineno = metrics.get("fail_user_frame_lineno")
            if filename is not None:
                source = f"{filename}:{lineno or 0}"
            self.failures_and_restarts.append(
                {
                    "kind": "failure",
                    "compile_id": compile_entry["compile_id"],
                    "compile_dir": compile_entry["compile_dir"],
                    "rank": compile_entry["rank"],
                    "failure_type": str(metrics.get("fail_type")),
                    "reason": str(metrics.get("fail_reason") or ""),
                    "source": source,
                }
            )

    def record_export_guard_failure(
        self,
        compile_entry: dict[str, Any] | None,
        event: ParsedEvent,
        *,
        failure_type: str,
        reason: str,
    ) -> None:
        detail = self.build_guard_detail(event, failure_type)
        self.guard_details.append(detail)
        self.export_failures.append(
            {
                "failure_type": failure_type,
                "reason": reason,
                "detail_id": detail["id"],
                "compile_id": event.compile_id,
                "compile_dir": event.compile_dir,
                "rank": event.rank,
            }
        )
        if compile_entry is not None:
            compile_entry["export_guards"].append(detail["id"])

    def build_guard_detail(self, event: ParsedEvent, failure_type: str) -> dict[str, Any]:
        metadata = (
            event.envelope.get("guard_added")
            or event.envelope.get("propagate_real_tensors_provenance")
            or {}
        )
        expr_node_id = metadata.get("expr_node_id")
        detail_id = f"guard-{len(self.guard_details) + 1:04d}"
        return {
            "id": detail_id,
            "failure_type": failure_type,
            "compile_id": event.compile_id,
            "compile_dir": event.compile_dir,
            "rank": event.rank,
            "expr": metadata.get("expr"),
            "result": metadata.get("result"),
            "symbol_to_sources": normalize_jsonable(metadata.get("symbol_to_sources") or {}),
            "frame_locals": normalize_jsonable(metadata.get("frame_locals") or {}),
            "user_stack": decode_stack(metadata.get("user_stack"), self.string_table),
            "framework_stack": decode_stack(metadata.get("stack"), self.string_table),
            "expression_tree": self.build_expression_tree(expr_node_id, set()),
        }

    def build_expression_tree(
        self,
        expr_id: Any,
        visited: set[int],
    ) -> dict[str, Any] | None:
        if not isinstance(expr_id, int):
            return None
        if expr_id in visited:
            return None
        info = self.sym_expr_info.get(expr_id)
        if info is None:
            return None
        visited.add(expr_id)
        argument_ids = info.get("argument_ids") or []
        return {
            "id": expr_id,
            "result": info.get("result"),
            "method": info.get("method"),
            "arguments": normalize_jsonable(info.get("arguments") or []),
            "user_stack": decode_stack(info.get("user_stack"), self.string_table),
            "framework_stack": decode_stack(info.get("stack"), self.string_table),
            "children": [
                child
                for child in (
                    self.build_expression_tree(argument_id, visited)
                    for argument_id in argument_ids
                )
                if child is not None
            ],
        }

    def record_chromium_event(self, event: ParsedEvent) -> None:
        try:
            parsed = json.loads(event.payload)
        except json.JSONDecodeError:
            return
        if isinstance(parsed, dict):
            if event.rank is not None and "pid" not in parsed:
                parsed["pid"] = event.rank
            self.chromium_events.append(parsed)

    def record_runtime_and_tensor_meta(
        self,
        event: ParsedEvent,
        artifact: dict[str, Any],
        compile_entry: dict[str, Any] | None,
    ) -> None:
        try:
            parsed = json.loads(read_artifact_content(self.artifacts_root, artifact["relative_path"]))
        except json.JSONDecodeError:
            return
        if not isinstance(parsed, dict):
            return

        graph_id = event.compile_id or event.compile_dir or artifact["title"]
        if event.rank is not None:
            ops = []
            for op in parsed.get("ops") or []:
                if not isinstance(op, dict):
                    continue
                runtime_ns = to_float(op.get("estimated_runtime_ns"))
                ops.append(
                    {
                        "name": str(op.get("name") or ""),
                        "estimated_runtime_ns": runtime_ns,
                    }
                )
            if ops:
                total_runtime_ns = sum(op["estimated_runtime_ns"] for op in ops)
                entry = {
                    "rank": event.rank,
                    "graph": graph_id,
                    "compile_id": event.compile_id,
                    "compile_dir": event.compile_dir,
                    "artifact_id": artifact["id"],
                    "ops": ops,
                    "total_runtime_ns": total_runtime_ns,
                }
                self.runtime_estimations.append(entry)
                if compile_entry is not None:
                    compile_entry["runtime_estimation"] = {
                        "artifact_id": artifact["id"],
                        "total_runtime_ns": total_runtime_ns,
                        "op_count": len(ops),
                    }

            canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
            digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            self.tensor_meta_signatures.append(
                {
                    "rank": event.rank,
                    "graph": graph_id,
                    "compile_id": event.compile_id,
                    "compile_dir": event.compile_dir,
                    "artifact_id": artifact["id"],
                    "signature_sha256": digest,
                }
            )

    def record_collective_schedule(
        self,
        event: ParsedEvent,
        artifact: dict[str, Any],
        compile_entry: dict[str, Any] | None,
    ) -> None:
        try:
            parsed = json.loads(read_artifact_content(self.artifacts_root, artifact["relative_path"]))
        except json.JSONDecodeError:
            return
        if not isinstance(parsed, list):
            return
        ops = [str(item) for item in parsed]
        entry = {
            "rank": event.rank,
            "graph": event.compile_id or event.compile_dir or artifact["title"],
            "compile_id": event.compile_id,
            "compile_dir": event.compile_dir,
            "artifact_id": artifact["id"],
            "ops": ops,
        }
        self.collective_schedules.append(entry)
        if compile_entry is not None:
            compile_entry["collective_schedule"] = {
                "artifact_id": artifact["id"],
                "ops": ops,
            }

    def record_execution_order(self, event: ParsedEvent, artifact: dict[str, Any]) -> None:
        if event.rank is None:
            return
        try:
            parsed = json.loads(read_artifact_content(self.artifacts_root, artifact["relative_path"]))
        except json.JSONDecodeError:
            return
        order = parse_graph_execution_order_payload(parsed)
        if order:
            self.exec_orders[event.rank] = order

    def record_vllm_config(self, payload: str) -> None:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return
        if isinstance(parsed, dict):
            self.vllm_state["config"] = normalize_jsonable(parsed)
            self.vllm_state["has_artifacts"] = True

    def record_vllm_subgraph(self, payload: str) -> None:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return
        if not isinstance(parsed, dict):
            return
        subgraph = {
            "index": parsed.get("piecewise_index"),
            "submod_name": parsed.get("submod_name"),
            "compile_range_start": parsed.get("compile_range_start"),
            "compile_range_end": parsed.get("compile_range_end"),
            "is_single_size": bool(parsed.get("is_single_size")),
            "is_cudagraph_size": bool(parsed.get("is_cudagraph_capture_size")),
            "artifacts": [],
        }
        self.vllm_state["subgraphs"].append(subgraph)
        self.vllm_state["current_subgraph"] = subgraph
        self.vllm_state["has_artifacts"] = True

    def record_vllm_artifact(self, name: str, artifact: dict[str, Any]) -> None:
        self.vllm_state["has_artifacts"] = True
        current = self.vllm_state.get("current_subgraph")
        if current is not None and (name.startswith("vllm_subgraph_") or name.startswith("vllm_submod_")):
            current["artifacts"].append(artifact)

    def record_raw_row(self, event: ParsedEvent, payload_filename: str | None) -> None:
        payload = json.loads(json.dumps(event.envelope))
        payload["timestamp"] = event.timestamp
        payload["thread"] = event.thread
        payload["pathname"] = event.pathname
        payload["lineno"] = event.source_line_no
        payload["log_file"] = event.log_file
        payload["log_line_no"] = event.log_line_no
        if payload_filename is not None:
            payload["payload_filename"] = payload_filename
        self.raw_rows.append(json.dumps(payload, sort_keys=True))

    def get_compile_entry(self, event: ParsedEvent) -> dict[str, Any] | None:
        if event.compile_dir is None and event.compile_id is None:
            return None
        key = event.compile_dir or f"unknown_{event.log_line_no}"
        if key not in self.compiles:
            self.compiles[key] = {
                "key": key,
                "compile_dir": key,
                "compile_id": event.compile_id or f"(unknown @ line {event.log_line_no})",
                "rank": event.rank,
                "first_line": event.log_line_no,
                "last_line": event.log_line_no,
                "status": "missing",
                "stack": [],
                "artifacts": [],
                "artifact_count": 0,
                "links": [],
                "compilation_metrics": None,
                "bwd_compilation_metrics": None,
                "aot_autograd_backward_compilation_metrics": None,
                "symbolic_shape_specializations": [],
                "guards_added_fast": [],
                "create_symbols": [],
                "unbacked_symbols": [],
                "export_guards": [],
                "collective_schedule": None,
                "collectives_parity": None,
                "runtime_estimation": None,
                "inductor_output_code_collectives": [],
                "inductor_output_code_artifact_id": None,
                "cache_status": None,
                "event_types": set(),
            }
        entry = self.compiles[key]
        entry["first_line"] = min(entry["first_line"], event.log_line_no)
        entry["last_line"] = max(entry["last_line"], event.log_line_no)
        if entry["rank"] is None:
            entry["rank"] = event.rank
        if entry["compile_id"].startswith("(unknown") and event.compile_id is not None:
            entry["compile_id"] = event.compile_id
        return entry

    def build_event_relative_path(self, event: ParsedEvent, plan: ArtifactPlan) -> Path:
        parts: list[str] = []
        if event.rank is not None:
            parts.append(f"rank_{event.rank}")
        if event.compile_dir is not None:
            parts.append(f"compile_{event.compile_dir}")
        return Path(*parts) / plan.subpath

    def write_artifact(
        self,
        *,
        family: str,
        kind: str,
        title: str,
        content: str,
        candidate_path: Path,
        encoding: str,
        content_type: str,
        event_type: str,
        rank: int | None,
        compile_id: str | None,
        compile_dir: str | None,
        log_file: str,
        line_no: int,
        summary: dict[str, Any] | None = None,
    ) -> tuple[ArtifactCreate, dict[str, Any]]:
        scoped_family = family if rank is None else f"rank:{rank}/{family}"
        self.family_counts[scoped_family] += 1
        family_index = self.family_counts[scoped_family]
        match_key = f"{scoped_family}@{family_index}"
        relative_path = dedupe_relative_path(candidate_path, self.used_paths)
        absolute_path = self.artifacts_root / relative_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        absolute_path.write_text(content, encoding="utf-8")

        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        artifact_summary = summary or build_summary(kind, content)
        artifact = ArtifactCreate(
            id=uuid.uuid4().hex,
            run_id=self.run_id,
            match_key=match_key,
            family=scoped_family,
            family_index=family_index,
            kind=kind,
            title=format_title(title, rank, compile_id, family_index),
            event_type=event_type,
            relative_path=relative_path.as_posix(),
            compile_id=compile_id,
            compile_dir=compile_dir,
            rank=rank,
            log_file=log_file,
            line_no=line_no,
            encoding=encoding,
            content_type=content_type,
            size_bytes=len(content.encode("utf-8")),
            sha256=digest,
            summary=artifact_summary,
        )
        descriptor = artifact_descriptor(artifact, self.artifact_number)
        self.artifact_number += 1
        self.artifacts.append(artifact)
        if event_type == "report":
            self.report_artifacts.append(descriptor)
        return artifact, descriptor

    def finalize_reports(self) -> None:
        self.write_raw_jsonl_report()
        self.write_compile_directory_report()
        if self.failures_and_restarts:
            self.write_json_report(
                family="report:failures_and_restarts",
                title="failures_and_restarts",
                filename="reports/failures_and_restarts.json",
                payload=self.failures_and_restarts,
            )
        if self.export_failures or self.exported_program_artifact_id is not None:
            self.write_json_report(
                family="report:export_report",
                title="export_report",
                filename="reports/export_report.json",
                payload={
                    "success": not self.export_failures,
                    "failures": self.export_failures,
                    "exported_program_artifact_id": self.exported_program_artifact_id,
                },
            )
        if self.chromium_events:
            self.write_json_report(
                family="report:chromium_events",
                title="chromium_events",
                filename="reports/chromium_events.json",
                payload=self.chromium_events,
            )
        if self.runtime_estimations:
            self.write_json_report(
                family="report:runtime_estimations",
                title="runtime_estimations",
                filename="reports/runtime_estimations.json",
                payload=self.runtime_estimations,
            )
            self.write_json_report(
                family="report:runtime_trace",
                title="chromium_trace_with_runtime",
                filename="reports/chromium_trace_with_runtime.json",
                payload=build_runtime_trace_events(self.runtime_estimations),
            )
        if self.collective_schedules:
            self.write_json_report(
                family="report:collective_schedules",
                title="collective_schedules",
                filename="reports/collective_schedules.json",
                payload=self.collective_schedules,
            )

        parity = self.build_collectives_parity()
        if parity:
            self.write_json_report(
                family="report:collectives_parity",
                title="collectives_parity",
                filename="reports/collectives_parity.json",
                payload=parity,
            )

        if self.tensor_meta_signatures:
            self.write_json_report(
                family="report:tensor_meta_signatures",
                title="tensor_meta_signatures",
                filename="reports/tensor_meta_signatures.json",
                payload=self.tensor_meta_signatures,
            )

        if self.exec_orders:
            self.write_json_report(
                family="report:execution_order",
                title="execution_order_report",
                filename="reports/execution_order_report.json",
                payload=build_exec_order_report(
                    self.exec_orders,
                    self.collective_schedules,
                    self.compiles,
                ),
            )

        multi_rank = self.build_multi_rank_summary()
        if multi_rank is not None:
            self.write_json_report(
                family="report:multi_rank_diagnostics",
                title="multi_rank_diagnostics",
                filename="reports/multi_rank_diagnostics.json",
                payload=multi_rank,
            )

        if self.vllm_state["has_artifacts"]:
            self.write_json_report(
                family="report:vllm_summary",
                title="vllm_summary",
                filename="reports/vllm_summary.json",
                payload=self.build_vllm_summary(),
            )

    def write_raw_jsonl_report(self) -> None:
        string_table_line = json.dumps({"string_table": build_string_table_list(self.string_table)})
        payload = "\n".join([string_table_line, *self.raw_rows]) if self.raw_rows else string_table_line
        self.write_artifact(
            family="report:raw_jsonl",
            kind="jsonl",
            title="raw.jsonl",
            content=payload,
            candidate_path=Path("reports/raw.jsonl"),
            encoding="jsonl",
            content_type="application/x-ndjson",
            event_type="report",
            rank=None,
            compile_id=None,
            compile_dir=None,
            log_file="",
            line_no=0,
            summary={
                "line_count": len(self.raw_rows) + 1,
                "preview": "string_table + shortraw events",
            },
        )

    def write_compile_directory_report(self) -> None:
        payload = {}
        for compile_entry in self.sorted_compiles():
            payload[compile_entry["compile_id"]] = {
                "compile_dir": compile_entry["compile_dir"],
                "rank": compile_entry["rank"],
                "status": compile_entry["status"],
                "artifact_count": compile_entry["artifact_count"],
                "artifacts": [
                    {
                        "id": artifact["id"],
                        "title": artifact["title"],
                        "name": artifact["relative_path"].split("/")[-1],
                        "url": artifact["relative_path"],
                        "number": artifact["number"],
                        "kind": artifact["kind"],
                        "summary": artifact["summary"],
                    }
                    for artifact in compile_entry["artifacts"]
                ],
                "links": compile_entry["links"],
            }
        self.write_json_report(
            family="report:compile_directory",
            title="compile_directory",
            filename="reports/compile_directory.json",
            payload=payload,
        )

    def write_json_report(
        self,
        *,
        family: str,
        title: str,
        filename: str,
        payload: Any,
    ) -> None:
        self.write_artifact(
            family=family,
            kind="json",
            title=title,
            content=json.dumps(payload, indent=2, sort_keys=True),
            candidate_path=Path(filename),
            encoding="json",
            content_type="application/json",
            event_type="report",
            rank=None,
            compile_id=None,
            compile_dir=None,
            log_file="",
            line_no=0,
        )

    def build_collectives_parity(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for compile_entry in self.sorted_compiles():
            schedule = compile_entry.get("collective_schedule")
            code_ops = compile_entry.get("inductor_output_code_collectives") or []
            if schedule is None and not code_ops:
                continue
            schedule_ops = schedule["ops"] if schedule is not None else []
            wait_count = sum(1 for op in code_ops if "wait" in op)
            code_collective_count = len(code_ops)
            missing_waits = max(code_collective_count - wait_count, 0)
            mismatch = code_collective_count != len(schedule_ops)
            parity_entry = {
                "rank": compile_entry["rank"],
                "compile_id": compile_entry["compile_id"],
                "compile_dir": compile_entry["compile_dir"],
                "schedule_count": len(schedule_ops),
                "code_collective_count": code_collective_count,
                "wait_count": wait_count,
                "missing_waits": missing_waits,
                "schedule_ops": schedule_ops,
                "code_ops": code_ops,
                "mismatch": mismatch,
            }
            compile_entry["collectives_parity"] = parity_entry
            entries.append(parity_entry)
        return entries

    def build_multi_rank_summary(self) -> dict[str, Any] | None:
        ranks = sorted(
            {
                rank
                for rank in (
                    *(entry["rank"] for entry in self.compiles.values()),
                    *(item["rank"] for item in self.runtime_estimations),
                    *(item["rank"] for item in self.collective_schedules),
                    *self.exec_orders.keys(),
                )
                if rank is not None
            }
        )
        if len(ranks) < 2 and not self.chromium_events:
            return None

        compile_sequences = build_rank_compile_sequences(self.sorted_compiles())
        cache_sequences = build_rank_cache_sequences(self.sorted_compiles())
        collective_groups = build_rank_groupings(
            build_rank_collective_signatures(self.collective_schedules)
        )
        tensor_meta_groups = build_rank_groupings(
            build_rank_tensor_meta_signatures(self.tensor_meta_signatures)
        )
        compile_groups = build_rank_groupings(compile_sequences)
        cache_groups = build_rank_groupings(cache_sequences)
        runtime_analysis = analyze_graph_runtime_deltas(self.runtime_estimations)
        exec_order = build_exec_order_report(self.exec_orders, self.collective_schedules, self.compiles)

        return {
            "ranks": ranks,
            "num_ranks": len(ranks),
            "has_chromium_events": bool(self.chromium_events),
            "compile_id_groups": compile_groups,
            "cache_groups": cache_groups,
            "collective_groups": collective_groups,
            "tensor_meta_groups": tensor_meta_groups,
            "runtime_analysis": runtime_analysis,
            "exec_order": exec_order,
            "divergence": {
                "compile_ids": len(compile_groups) > 1,
                "cache": len(cache_groups) > 1,
                "collective": len(collective_groups) > 1,
                "tensor_meta": len(tensor_meta_groups) > 1,
            },
        }

    def build_vllm_summary(self) -> dict[str, Any]:
        return {
            "config": self.vllm_state["config"],
            "piecewise_graph": self.vllm_state["piecewise_graph"],
            "subgraphs": self.vllm_state["subgraphs"],
        }

    def build_manifest(self) -> dict[str, Any]:
        export_summary = None
        if self.export_failures or self.exported_program_artifact_id is not None:
            export_summary = {
                "success": not self.export_failures,
                "failures": self.export_failures,
                "exported_program_artifact_id": self.exported_program_artifact_id,
                "guard_details": self.guard_details,
            }

        multi_rank = self.build_multi_rank_summary()
        return {
            "run_id": self.run_id,
            "artifact_count": len(self.artifacts),
            "compile_count": len(self.compiles),
            "log_files": self.log_files,
            "warnings": self.warnings,
            "unknown_stacks": self.unknown_stacks,
            "compiles": self.sorted_compiles(),
            "failures_and_restarts": self.failures_and_restarts,
            "export": export_summary,
            "multi_rank": multi_rank,
            "report_artifact_ids": [artifact["id"] for artifact in self.report_artifacts],
            "has_vllm_artifacts": self.vllm_state["has_artifacts"],
            "vllm": self.build_vllm_summary() if self.vllm_state["has_artifacts"] else None,
        }

    def sorted_compiles(self) -> list[dict[str, Any]]:
        compiles = []
        for compile_entry in sorted(
            self.compiles.values(),
            key=lambda entry: (
                entry["rank"] if entry["rank"] is not None else -1,
                entry["first_line"],
                entry["compile_id"],
            ),
        ):
            normalized = dict(compile_entry)
            normalized["event_types"] = sorted(normalized["event_types"])
            compiles.append(normalized)
        return compiles


def ingest_trace_dir(run_id: str, trace_dir: Path, artifacts_root: Path) -> ParseResult:
    return TraceIngestor(run_id, trace_dir, artifacts_root).ingest()


def discover_trace_files(trace_dir: Path) -> list[Path]:
    if not trace_dir.exists():
        return []
    return sorted(
        (path for path in trace_dir.rglob("*") if path.is_file()),
        key=lambda path: path.as_posix(),
    )


def iter_trace_events(
    log_path: Path,
    relative_log: str,
    warnings: list[str],
    string_table: dict[int, str],
) -> Iterator[ParsedEvent]:
    with open_trace_file(log_path) as handle:
        buffered: tuple[int, str] | None = None
        line_iter = enumerate(handle, start=1)
        while True:
            if buffered is None:
                try:
                    log_line_no, raw_line = next(line_iter)
                except StopIteration:
                    break
            else:
                log_line_no, raw_line = buffered
                buffered = None

            line = raw_line.rstrip("\n")
            if not line:
                continue

            match = GLOG_RE.match(line)
            if match is None:
                warnings.append(f"{relative_log}:{log_line_no}: failed to parse glog prefix")
                continue

            try:
                envelope = json.loads(match.group("payload"))
            except json.JSONDecodeError as error:
                warnings.append(f"{relative_log}:{log_line_no}: invalid JSON envelope ({error})")
                continue

            if not isinstance(envelope, dict):
                warnings.append(f"{relative_log}:{log_line_no}: envelope is not a JSON object")
                continue

            if "str" in envelope and envelope.get("str") is not None:
                value = envelope.get("str")
                if isinstance(value, list) and len(value) == 2 and isinstance(value[1], int):
                    string_table[value[1]] = str(value[0])
                continue

            payload = ""
            expected_payload = envelope.get("has_payload")
            if expected_payload:
                payload_lines: list[str] = []
                while True:
                    try:
                        next_line_no, next_raw = next(line_iter)
                    except StopIteration:
                        break

                    next_line = next_raw.rstrip("\n")
                    if not next_line:
                        continue
                    if next_line.startswith("\t"):
                        payload_lines.append(next_line[1:])
                        continue

                    buffered = (next_line_no, next_raw)
                    break

                payload = "\n".join(payload_lines)
                observed = hashlib.md5(payload.encode("utf-8")).hexdigest()
                if observed != str(expected_payload):
                    warnings.append(
                        f"{relative_log}:{log_line_no}: payload hash mismatch "
                        f"(expected {expected_payload}, got {observed})"
                    )

            event_type = discover_event_type(envelope)
            compile_id, compile_dir = extract_compile_identity(envelope)
            rank = parse_int(envelope.get("rank"))
            source_line_no = int(match.group("source_line"))
            yield ParsedEvent(
                event_type=event_type,
                envelope=envelope,
                payload=payload,
                log_line_no=log_line_no,
                source_line_no=source_line_no,
                log_file=relative_log,
                rank=rank,
                compile_id=compile_id,
                compile_dir=compile_dir,
                timestamp=format_timestamp(match),
                thread=parse_int(match.group("thread")) or match.group("thread"),
                pathname=match.group("pathname"),
            )


def open_trace_file(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("rt", encoding="utf-8", errors="replace")


def discover_event_type(envelope: dict[str, Any]) -> str:
    interesting = [
        key for key, value in envelope.items() if value is not None and key not in STANDARD_KEYS
    ]
    if not interesting:
        if envelope.get("has_payload") is not None:
            return "payload"
        return "unknown"

    for key in (
        "artifact",
        "graph_dump",
        "inductor_output_code",
        "dump_file",
        "dynamo_guards",
        "memoizer_artifacts",
        "compilation_metrics",
        "bwd_compilation_metrics",
        "aot_autograd_backward_compilation_metrics",
        "link",
        "symbolic_shape_specialization",
        "guard_added_fast",
        "guard_added",
        "propagate_real_tensors_provenance",
        "create_symbol",
        "create_unbacked_symbol",
        "expression_created",
        "chromium_event",
    ):
        if key in interesting:
            return key
    return interesting[0]


def extract_compile_identity(envelope: dict[str, Any]) -> tuple[str | None, str | None]:
    compiled_autograd_id = envelope.get("compiled_autograd_id")
    frame_id = envelope.get("frame_id")
    frame_compile_id = envelope.get("frame_compile_id")
    attempt = envelope.get("attempt")
    if frame_compile_id is not None and attempt is None:
        attempt = 0

    if (
        compiled_autograd_id is None
        and frame_id is None
        and frame_compile_id is None
        and attempt is None
    ):
        return None, None

    compiled_text = str(compiled_autograd_id) if compiled_autograd_id is not None else "-"
    frame_text = str(frame_id) if frame_id is not None else "-"
    compile_text = str(frame_compile_id) if frame_compile_id is not None else "-"
    attempt_text = str(attempt) if attempt is not None else "-"

    prefix = f"!{compiled_autograd_id}/" if compiled_autograd_id is not None else ""
    display = f"[{prefix}{frame_text}/{compile_text}"
    if attempt not in (None, 0):
        display += f"_{attempt}"
    display += "]"
    directory = f"{compiled_text}_{frame_text}_{compile_text}_{attempt_text}"
    return display, directory


def build_artifact_plan(event: ParsedEvent) -> ArtifactPlan | None:
    envelope = event.envelope
    payload = event.payload
    event_type = event.event_type

    if event_type in SKIP_EVENTS:
        return None

    if event_type in GRAPH_EVENTS:
        return ArtifactPlan(
            family=event_type,
            kind="fx_graph",
            title=event_type,
            subpath=Path("graphs") / f"{event_type}.txt",
            content=payload,
            encoding="string",
            content_type="text/plain",
        )

    if event_type == "graph_dump":
        metadata = envelope.get("graph_dump") or {}
        name = str(metadata.get("name") or "graph_dump")
        return ArtifactPlan(
            family=f"graph_dump:{name}",
            kind="fx_graph" if "graph" in name.lower() else "text",
            title=name,
            subpath=Path("graph_dump") / f"{safe_slug(name)}.txt",
            content=payload,
            encoding="string",
            content_type="text/plain",
        )

    if event_type == "inductor_output_code":
        metadata = envelope.get("inductor_output_code") or {}
        filename = metadata.get("filename") or "inductor_output_code.py"
        stem = Path(str(filename)).stem or "inductor_output_code"
        return ArtifactPlan(
            family=f"inductor_output_code:{stem}",
            kind="python",
            title=f"inductor_output_code:{stem}",
            subpath=Path("code") / f"{safe_slug(stem)}.py",
            content=payload,
            encoding="string",
            content_type="text/x-python",
        )

    if event_type == "artifact":
        metadata = envelope.get("artifact") or {}
        name = str(metadata.get("name") or "artifact")
        encoding = str(metadata.get("encoding") or "string")
        if encoding == "json":
            content, parsed = pretty_json(payload)
            kind = "json" if parsed else "text"
            suffix = ".json" if parsed else ".txt"
            content_type = "application/json" if parsed else "text/plain"
        else:
            content = payload
            kind = infer_text_kind(name, payload)
            suffix = ".py" if kind == "python" else ".txt"
            content_type = "text/x-python" if kind == "python" else "text/plain"
        return ArtifactPlan(
            family=f"artifact:{name}",
            kind=kind,
            title=name,
            subpath=Path("artifacts") / f"{safe_slug(name)}{suffix}",
            content=content,
            encoding=encoding,
            content_type=content_type,
        )

    if event_type == "dynamo_guards":
        content, parsed = pretty_json(payload)
        return ArtifactPlan(
            family="dynamo_guards",
            kind="json" if parsed else "text",
            title="dynamo_guards",
            subpath=Path("diagnostics") / ("dynamo_guards.json" if parsed else "dynamo_guards.txt"),
            content=content,
            encoding="json" if parsed else "string",
            content_type="application/json" if parsed else "text/plain",
        )

    if event_type == "dynamo_cpp_guards_str":
        return ArtifactPlan(
            family="dynamo_cpp_guards_str",
            kind="text",
            title="dynamo_cpp_guards_str",
            subpath=Path("diagnostics") / "dynamo_cpp_guards_str.txt",
            content=payload,
            encoding="string",
            content_type="text/plain",
        )

    if event_type == "memoizer_artifacts":
        content, parsed = pretty_json(payload)
        return ArtifactPlan(
            family="memoizer_artifacts",
            kind="json" if parsed else "text",
            title="memoizer_artifacts",
            subpath=Path("artifacts")
            / ("memoizer_artifacts.json" if parsed else "memoizer_artifacts.txt"),
            content=content,
            encoding="json" if parsed else "string",
            content_type="application/json" if parsed else "text/plain",
        )

    if event_type == "dump_file":
        metadata = envelope.get("dump_file") or {}
        raw_name = str(metadata.get("name") or "dump_file.txt")
        source_name = Path(raw_name).name
        stem = Path(source_name).stem or "dump_file"
        suffix = Path(source_name).suffix or ".txt"
        kind = "python" if suffix == ".py" else "text"
        return ArtifactPlan(
            family=f"dump_file:{source_name}",
            kind=kind,
            title=source_name,
            subpath=Path("dump_files") / f"{safe_slug(stem)}{suffix}",
            content=payload,
            encoding="string",
            content_type="text/x-python" if kind == "python" else "text/plain",
        )

    if event_type == "exported_program":
        return ArtifactPlan(
            family="exported_program",
            kind="text",
            title="exported_program",
            subpath=Path("export") / "exported_program.txt",
            content=payload,
            encoding="string",
            content_type="text/plain",
        )

    if envelope.get("has_payload") is None:
        return None

    content, parsed = pretty_json(payload)
    kind = "json" if parsed else infer_text_kind(event_type, payload)
    suffix = ".json" if kind == "json" else ".py" if kind == "python" else ".txt"
    return ArtifactPlan(
        family=event_type,
        kind=kind,
        title=event_type,
        subpath=Path("payloads") / f"{safe_slug(event_type)}{suffix}",
        content=content if kind == "json" else payload,
        encoding="json" if kind == "json" else "string",
        content_type="application/json" if kind == "json" else "text/plain",
    )


def format_title(
    base_title: str,
    rank: int | None,
    compile_id: str | None,
    family_index: int,
) -> str:
    parts = [base_title]
    if rank is not None:
        parts.append(f"rank {rank}")
    if compile_id:
        parts.append(compile_id)
    if family_index > 1:
        parts.append(f"#{family_index}")
    return " | ".join(parts)


def build_summary(kind: str, content: str) -> dict[str, Any]:
    lines = content.splitlines()
    summary: dict[str, Any] = {"line_count": len(lines)}
    first_nonempty = next((line.strip() for line in lines if line.strip()), "")
    if first_nonempty:
        summary["preview"] = first_nonempty[:160]

    if kind == "json":
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            summary["json_valid"] = False
        else:
            summary["json_valid"] = True
            summary["json_type"] = type(parsed).__name__
            if isinstance(parsed, dict):
                summary["top_level_keys"] = len(parsed)
            elif isinstance(parsed, list):
                summary["top_level_items"] = len(parsed)
        return summary

    if kind == "jsonl":
        summary["entry_count"] = max(len(lines) - 1, 0)
        return summary

    if kind == "fx_graph":
        op_counts: Counter[str] = Counter()
        for raw_line in lines:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "placeholder[target" in stripped or "placeholder[" in stripped:
                op_counts["placeholder"] += 1
            elif "call_function[target" in stripped or "call_function[" in stripped:
                op_counts["call_function"] += 1
            elif "call_method[target" in stripped or "call_method[" in stripped:
                op_counts["call_method"] += 1
            elif "call_module[target" in stripped or "call_module[" in stripped:
                op_counts["call_module"] += 1
            elif "get_attr[target" in stripped or "get_attr[" in stripped:
                op_counts["get_attr"] += 1
            elif stripped.startswith("return ") or "output[target" in stripped or "output[" in stripped:
                op_counts["output"] += 1
        summary["node_count"] = sum(op_counts.values())
        summary["op_counts"] = dict(op_counts)
        return summary

    return summary


def pretty_json(payload: str) -> tuple[str, bool]:
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return payload, False
    return json.dumps(parsed, indent=2, sort_keys=True), True


def infer_text_kind(name: str, payload: str) -> str:
    lowered = name.lower()
    prefix = payload.lstrip()[:200]
    if "graph" in lowered:
        return "fx_graph"
    if "code" in lowered or prefix.startswith("def ") or prefix.startswith("from ") or prefix.startswith("import "):
        return "python"
    return "text"


def dedupe_relative_path(candidate: Path, used_paths: set[str]) -> Path:
    normalized = candidate.as_posix()
    if normalized not in used_paths:
        used_paths.add(normalized)
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    parent = candidate.parent
    index = 2
    while True:
        attempt = parent / f"{stem}__{index}{suffix}"
        normalized = attempt.as_posix()
        if normalized not in used_paths:
            used_paths.add(normalized)
            return attempt
        index += 1


def artifact_descriptor(artifact: ArtifactCreate, number: int) -> dict[str, Any]:
    return {
        "id": artifact.id,
        "title": artifact.title,
        "relative_path": artifact.relative_path,
        "match_key": artifact.match_key,
        "family": artifact.family,
        "kind": artifact.kind,
        "sha256": artifact.sha256,
        "summary": artifact.summary,
        "event_type": artifact.event_type,
        "number": number,
    }


def normalize_symbolic_shape_specialization(
    metadata: dict[str, Any],
    string_table: dict[int, str],
) -> dict[str, Any]:
    return {
        "symbol": metadata.get("symbol"),
        "sources": normalize_jsonable(metadata.get("sources") or []),
        "value": metadata.get("value"),
        "reason": metadata.get("reason"),
        "user_stack": decode_stack(metadata.get("user_stack"), string_table),
        "framework_stack": decode_stack(metadata.get("stack"), string_table),
    }


def normalize_guard_added_fast(
    metadata: dict[str, Any],
    string_table: dict[int, str],
) -> dict[str, Any]:
    return {
        "expr": metadata.get("expr"),
        "user_stack": decode_stack(metadata.get("user_stack"), string_table),
        "framework_stack": decode_stack(metadata.get("stack"), string_table),
    }


def normalize_symbol_record(
    metadata: dict[str, Any],
    string_table: dict[int, str],
) -> dict[str, Any]:
    return {
        "symbol": metadata.get("symbol"),
        "val": metadata.get("val"),
        "vr": metadata.get("vr"),
        "source": metadata.get("source"),
        "user_stack": decode_stack(metadata.get("user_stack"), string_table),
        "framework_stack": decode_stack(metadata.get("stack"), string_table),
    }


def normalize_unbacked_symbol_record(
    metadata: dict[str, Any],
    string_table: dict[int, str],
) -> dict[str, Any]:
    return {
        "symbol": metadata.get("symbol"),
        "vr": metadata.get("vr"),
        "user_stack": decode_stack(metadata.get("user_stack"), string_table),
        "framework_stack": decode_stack(metadata.get("stack"), string_table),
    }


def decode_stack(raw_stack: Any, string_table: dict[int, str]) -> list[dict[str, Any]]:
    if not isinstance(raw_stack, list):
        return []
    decoded: list[dict[str, Any]] = []
    for frame in raw_stack:
        if not isinstance(frame, dict):
            continue
        filename: str
        if frame.get("uninterned_filename"):
            filename = str(frame.get("uninterned_filename"))
        else:
            filename = string_table.get(parse_int(frame.get("filename")) or -1, "(unknown)")
        decoded.append(
            {
                "filename": simplify_filename(filename),
                "line": parse_int(frame.get("line")) or 0,
                "name": str(frame.get("name") or ""),
                "loc": str(frame.get("loc") or ""),
            }
        )
    return decoded


def maybe_remove_convert_frame_suffixes(stack: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not stack:
        return stack
    trimmed = list(stack)
    for targets in CONVERT_FRAME_SUFFIXES:
        if len(trimmed) < len(targets):
            continue
        suffix = trimmed[-len(targets) :]
        if all(
            suffix[index]["filename"].endswith(targets[index][0])
            and suffix[index]["name"] == targets[index][1]
            for index in range(len(targets))
        ):
            trimmed = trimmed[: -len(targets)]
    return trimmed


def simplify_filename(filename: str) -> str:
    if "#link-tree/" in filename:
        return filename.split("#link-tree/", 1)[1]
    match = SEED_NSPID_RE.search(filename)
    if match:
        return filename[match.end() :]
    return filename


def compute_compile_status(metrics: dict[str, Any]) -> str:
    if metrics.get("fail_type"):
        return "error"
    restart_reasons = metrics.get("restart_reasons") or []
    if restart_reasons:
        return "break"
    graph_op_count = metrics.get("graph_op_count")
    if isinstance(graph_op_count, int) and graph_op_count == 0:
        return "empty"
    return "ok"


def update_cache_status(compile_entry: dict[str, Any], name: str) -> None:
    priorities = {"cache_bypass": 1, "cache_hit": 2, "cache_miss": 3}
    selected = None
    selected_priority = -1
    for label, priority in priorities.items():
        if label in name and priority > selected_priority:
            selected = label.replace("cache_", "")
            selected_priority = priority
    if selected is None:
        return
    current = compile_entry.get("cache_status")
    current_priority = priorities.get(f"cache_{current}", -1) if current else -1
    if selected_priority > current_priority:
        compile_entry["cache_status"] = selected


def extract_collective_calls(payload: str) -> list[str]:
    stripped = HTML_TAG_RE.sub("", COMMENT_RE.sub("", payload))
    return [match.group(1) for match in COLLECTIVE_CALL_RE.finditer(stripped)]


def parse_graph_execution_order_payload(parsed: Any) -> list[str]:
    if not isinstance(parsed, dict):
        return []
    raw = parsed.get("graph_execution_order")
    if not isinstance(raw, list):
        return []
    order: list[str] = []
    for item in raw:
        if isinstance(item, str):
            order.append(normalize_compile_id_string(item))
        elif isinstance(item, dict):
            compile_id = item.get("compile_id")
            if compile_id is not None:
                order.append(normalize_compile_id_string(str(compile_id)))
    return order


def normalize_compile_id_string(value: str) -> str:
    stripped = value.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        return stripped
    return f"[{stripped}]"


def build_runtime_trace_events(runtime_estimations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    threads: dict[tuple[int, str], int] = {}
    next_tid = 1
    for runtime in runtime_estimations:
        rank = runtime["rank"]
        graph = runtime["graph"]
        key = (rank, graph)
        if key not in threads:
            threads[key] = next_tid
            next_tid += 1
        tid = threads[key]
        current_ts_us = 0
        for op in runtime["ops"]:
            dur_us = max(int((to_float(op.get("estimated_runtime_ns")) / 1000.0) or 0), 1)
            events.append(
                {
                    "name": op.get("name") or "op",
                    "ph": "X",
                    "pid": rank,
                    "tid": tid,
                    "ts": current_ts_us,
                    "dur": dur_us,
                    "cat": "runtime",
                    "args": {"graph": graph, "runtime_ns": int(to_float(op.get("estimated_runtime_ns")))},
                }
            )
            current_ts_us += dur_us
    for (rank, graph), tid in threads.items():
        events.append(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": rank,
                "tid": tid,
                "args": {"name": f"graph {graph}"},
            }
        )
    return events


def analyze_graph_runtime_deltas(runtime_estimations: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not runtime_estimations:
        return None
    by_rank: dict[int, list[tuple[str, float]]] = defaultdict(list)
    for runtime in runtime_estimations:
        rank = runtime.get("rank")
        if rank is None:
            continue
        total = sum(to_float(op.get("estimated_runtime_ns")) for op in runtime.get("ops") or [])
        by_rank[int(rank)].append((str(runtime.get("graph")), total))
    if len(by_rank) < 2:
        return None

    lengths = {len(graphs) for graphs in by_rank.values()}
    if len(lengths) != 1:
        return {"graphs": [], "has_mismatched_graph_counts": True}

    graphs = []
    ranks = sorted(by_rank)
    graph_count = next(iter(lengths))
    for index in range(graph_count):
        runtimes = []
        for rank in ranks:
            graph_id, runtime_ns = by_rank[rank][index]
            runtimes.append((rank, graph_id, runtime_ns))
        fastest = min(runtimes, key=lambda item: item[2])
        slowest = max(runtimes, key=lambda item: item[2])
        graphs.append(
            {
                "graph_index": index,
                "graph_id": runtimes[0][1],
                "delta_ms": round((slowest[2] - fastest[2]) / 1e6, 3),
                "rank_details": [
                    {"rank": fastest[0], "runtime_ms": round(fastest[2] / 1e6, 3)},
                    {"rank": slowest[0], "runtime_ms": round(slowest[2] / 1e6, 3)},
                ],
            }
        )
    graphs.sort(key=lambda item: item["graph_id"])
    return {"graphs": graphs, "has_mismatched_graph_counts": False}


def build_exec_order_report(
    exec_orders: dict[int, list[str]],
    collective_schedules: list[dict[str, Any]],
    compiles: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if len(exec_orders) < 2:
        return None
    collective_by_compile = {
        (entry.get("rank"), entry.get("compile_id") or entry.get("compile_dir")): entry.get("ops") or []
        for entry in collective_schedules
    }
    cache_by_compile = {
        (entry.get("rank"), entry.get("compile_id")): entry.get("cache_status")
        for entry in compiles.values()
        if entry.get("compile_id")
    }
    max_len = max(len(order) for order in exec_orders.values())
    rows = []
    ranks_with_schedule: set[int] = set()
    ranks_with_cache: set[int] = set()
    for index in range(max_len):
        by_rank = {
            rank: order[index]
            for rank, order in exec_orders.items()
            if index < len(order)
        }
        if not by_rank:
            continue
        schedules = [
            collective_by_compile.get((rank, compile_id), [])
            for rank, compile_id in by_rank.items()
        ]
        caches = [
            cache_by_compile.get((rank, compile_id))
            for rank, compile_id in by_rank.items()
            if cache_by_compile.get((rank, compile_id))
        ]
        issues = []
        if len(schedules) >= 2 and schedules[1:] and any(schedule != schedules[0] for schedule in schedules[1:]):
            issues.append("schedule_mismatch")
            ranks_with_schedule.update(by_rank.keys())
        if len(caches) >= 2 and caches[1:] and any(cache != caches[0] for cache in caches[1:]):
            issues.append("cache_mismatch")
            ranks_with_cache.update(by_rank.keys())
        rows.append({"index": index, "by_rank": by_rank, "issues": issues})

    order_differs = any(len(set(row["by_rank"].values())) > 1 for row in rows)
    return {
        "by_index": rows,
        "order_differs": order_differs,
        "has_schedule_mismatch": bool(ranks_with_schedule),
        "has_cache_mismatch": bool(ranks_with_cache),
        "ranks_schedule": sorted(ranks_with_schedule),
        "ranks_cache": sorted(ranks_with_cache),
        "ranks_schedule_str": ", ".join(f"Rank {rank}" for rank in sorted(ranks_with_schedule)),
        "ranks_cache_str": ", ".join(f"Rank {rank}" for rank in sorted(ranks_with_cache)),
    }


def build_rank_compile_sequences(compiles: list[dict[str, Any]]) -> dict[int, str]:
    by_rank: dict[int, list[str]] = defaultdict(list)
    for compile_entry in compiles:
        rank = compile_entry.get("rank")
        if rank is None:
            continue
        by_rank[int(rank)].append(str(compile_entry.get("compile_id")))
    return {rank: json.dumps(sequence) for rank, sequence in by_rank.items()}


def build_rank_cache_sequences(compiles: list[dict[str, Any]]) -> dict[int, str]:
    by_rank: dict[int, list[str]] = defaultdict(list)
    for compile_entry in compiles:
        rank = compile_entry.get("rank")
        if rank is None:
            continue
        by_rank[int(rank)].append(str(compile_entry.get("cache_status") or "unknown"))
    return {rank: json.dumps(sequence) for rank, sequence in by_rank.items()}


def build_rank_collective_signatures(collective_schedules: list[dict[str, Any]]) -> dict[int, str]:
    by_rank: dict[int, list[list[str]]] = defaultdict(list)
    for schedule in collective_schedules:
        rank = schedule.get("rank")
        if rank is None:
            continue
        by_rank[int(rank)].append([str(item) for item in schedule.get("ops") or []])
    return {rank: json.dumps(sequence) for rank, sequence in by_rank.items()}


def build_rank_tensor_meta_signatures(entries: list[dict[str, Any]]) -> dict[int, str]:
    by_rank: dict[int, list[str]] = defaultdict(list)
    for entry in entries:
        rank = entry.get("rank")
        if rank is None:
            continue
        by_rank[int(rank)].append(str(entry.get("signature_sha256")))
    return {rank: json.dumps(sequence) for rank, sequence in by_rank.items()}


def build_rank_groupings(signatures: dict[int, str]) -> list[dict[str, Any]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for rank, signature in signatures.items():
        grouped[signature].append(rank)
    return [
        {
            "signature": signature,
            "ranks": ", ".join(str(rank) for rank in sorted(ranks)),
            "rank_list": sorted(ranks),
        }
        for signature, ranks in grouped.items()
    ]


def build_string_table_list(string_table: dict[int, str]) -> list[str | None]:
    if not string_table:
        return []
    max_index = max(string_table)
    table: list[str | None] = [None] * (max_index + 1)
    for index, value in string_table.items():
        if index >= 0:
            table[index] = value
    return table


def read_artifact_content(root: Path, relative_path: str) -> str:
    return (root / relative_path).read_text(encoding="utf-8", errors="replace")


def format_timestamp(match: re.Match[str]) -> str:
    year = datetime.now().year
    return (
        f"{year:04d}-{match.group('month')}-{match.group('day')}"
        f"T{match.group('hour')}:{match.group('minute')}:{match.group('second')}.{match.group('microsecond')}"
    )


def parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value))


def safe_slug(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    normalized = []
    for char in value.strip():
        normalized.append(char if char in allowed else "_")
    slug = "".join(normalized).strip("._")
    return slug or "artifact"

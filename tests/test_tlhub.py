from __future__ import annotations

from contextlib import contextmanager
import hashlib
import http.client
import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import threading
import unittest
from unittest import mock
from http.server import ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tlhub import cli  # noqa: E402
from tlhub.config import BASE_URL_ENV, DEFAULT_PORT, build_app_url, build_user_url, get_paths  # noqa: E402
from tlhub.database import Repository, RunCreate  # noqa: E402
from tlhub import server  # noqa: E402
from tlhub.trace_parser import ingest_trace_dir  # noqa: E402
from tlhub.view_helpers import build_provenance_groups  # noqa: E402


def emit_event(prefix: str, payload: str, *, extra_lines: str = "") -> str:
    normalized_payload = "\n".join(payload.splitlines())
    digest = hashlib.md5(normalized_payload.encode("utf-8")).hexdigest()
    metadata = f'{prefix}, "has_payload": "{digest}"'
    payload_block = "\n".join(f"\t{line}" for line in normalized_payload.splitlines())
    return (
        "V0401 12:00:00.000001 123 torch/_logging/structured.py:27] "
        f"{{{metadata}}}\n"
        f"{payload_block}\n"
        f"{extra_lines}"
    )


def stage_fixture(trace_dir: Path, fixture_path: Path) -> None:
    if fixture_path.is_dir():
        for source in fixture_path.rglob("*"):
            if not source.is_file():
                continue
            target = trace_dir / source.relative_to(fixture_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
        return
    (trace_dir / fixture_path.name).write_bytes(fixture_path.read_bytes())


def index_run(
    repo: Repository,
    run_id: str,
    trace_dir: Path,
    artifacts_dir: Path,
):
    created_at = "2026-04-17T12:00:00"
    repo.create_run(
        RunCreate(
            id=run_id,
            status="running",
            command=["pytest"],
            command_display="pytest",
            cwd=str(trace_dir.parent),
            hostname="test-host",
            trace_dir=str(trace_dir),
            created_at=created_at,
            started_at=created_at,
        )
    )
    result = ingest_trace_dir(run_id, trace_dir, artifacts_dir)
    repo.finish_run(
        run_id,
        status="finished",
        finished_at="2026-04-17T12:00:10",
        duration_ms=1000,
        exit_code=0,
        log_count=len(result.log_files),
        artifacts=result.artifacts,
    )
    return result


@contextmanager
def run_http_server(paths, repo):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.make_handler(paths, repo))
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield httpd
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


class TLHubTests(unittest.TestCase):
    def test_cli_parser_defaults_to_port_44107(self) -> None:
        self.assertEqual(DEFAULT_PORT, 44107)
        self.assertEqual(cli.build_parser().parse_args([]).port, 44107)

    def test_build_user_url_uses_base_url_host_path_and_runtime_port(self) -> None:
        with mock.patch.dict(
            os.environ,
            {BASE_URL_ENV: "https://proxy.example.com/reviewer?via=ssh"},
            clear=False,
        ):
            self.assertEqual(
                build_user_url(9237),
                "https://proxy.example.com:9237/reviewer?via=ssh",
            )
            self.assertEqual(
                build_user_url(9237, "/runs/run-1"),
                "https://proxy.example.com:9237/reviewer/runs/run-1?via=ssh",
            )

    def test_build_user_url_rejects_invalid_base_url(self) -> None:
        with mock.patch.dict(os.environ, {BASE_URL_ENV: "proxy.example.com"}, clear=False):
            with self.assertRaisesRegex(
                ValueError,
                "BASE_URL must be a full URL like http://example.com.",
            ):
                build_user_url(9234)

    def test_build_app_url_preserves_base_query(self) -> None:
        with mock.patch.dict(
            os.environ,
            {BASE_URL_ENV: "https://proxy.example.com/reviewer?via=ssh"},
            clear=False,
        ):
            self.assertEqual(
                build_app_url("/compare", query={"left_run": "run-1"}),
                "/reviewer/compare?via=ssh&left_run=run-1",
            )

    def test_ingest_trace_dir_extracts_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = Path(tmpdir) / "trace"
            artifacts_dir = Path(tmpdir) / "artifacts"
            trace_dir.mkdir()

            graph_payload = "graph():\n    %x = placeholder[target=x]\n    return x"
            json_payload = '{"ops": [{"name": "mm", "estimated_runtime_ns": 12.0}]}'
            code_payload = "def call(args):\n    return args\n"
            log_text = "".join(
                [
                    emit_event('"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0', graph_payload),
                    emit_event('"artifact": {"name": "inductor_runtime_and_tensor_meta", "encoding": "json"}, "rank": 0, "frame_id": 0, "frame_compile_id": 0, "attempt": 0', json_payload),
                    emit_event('"inductor_output_code": {"filename": "fx_graph_runnable.py"}, "rank": 0, "frame_id": 0, "frame_compile_id": 0, "attempt": 0', code_payload),
                ]
            )
            (trace_dir / "trace.log").write_text(log_text, encoding="utf-8")

            result = ingest_trace_dir("run-1", trace_dir, artifacts_dir)
            primary_artifacts = [
                artifact for artifact in result.artifacts if not artifact.family.startswith("report:")
            ]
            report_artifacts = [
                artifact for artifact in result.artifacts if artifact.family.startswith("report:")
            ]

            self.assertEqual(len(primary_artifacts), 3)
            self.assertGreaterEqual(len(report_artifacts), 2)
            self.assertEqual(result.log_files, ["trace.log"])
            self.assertEqual(result.warnings, [])
            self.assertEqual(result.manifest["compile_count"], 1)

            families = {artifact.family for artifact in primary_artifacts}
            self.assertIn("dynamo_output_graph", families)
            self.assertIn("rank:0/artifact:inductor_runtime_and_tensor_meta", families)
            self.assertIn("rank:0/inductor_output_code:fx_graph_runnable", families)
            self.assertIn("report:raw_jsonl", {artifact.family for artifact in report_artifacts})
            self.assertIn("report:compile_directory", {artifact.family for artifact in report_artifacts})

            graph_artifact = next(artifact for artifact in primary_artifacts if artifact.kind == "fx_graph")
            self.assertGreater(graph_artifact.summary["node_count"], 0)
            stored_graph = (artifacts_dir / graph_artifact.relative_path).read_text(encoding="utf-8")
            self.assertIn("placeholder[target=x]", stored_graph)

            json_artifact = next(artifact for artifact in primary_artifacts if artifact.kind == "json")
            stored_json = (artifacts_dir / json_artifact.relative_path).read_text(encoding="utf-8")
            self.assertIn('"ops"', stored_json)
            self.assertIn("\n  ", stored_json)
            raw_jsonl = next(artifact for artifact in report_artifacts if artifact.family == "report:raw_jsonl")
            raw_text = (artifacts_dir / raw_jsonl.relative_path).read_text(encoding="utf-8")
            self.assertIn('"string_table"', raw_text)

    def test_repeated_family_gets_incremented_match_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = Path(tmpdir) / "trace"
            artifacts_dir = Path(tmpdir) / "artifacts"
            trace_dir.mkdir()

            graph_one = "graph():\n    %x = placeholder[target=x]\n    return x"
            graph_two = "graph():\n    %y = placeholder[target=y]\n    return y"
            log_text = "".join(
                [
                    emit_event('"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0', graph_one),
                    emit_event('"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 1, "attempt": 0', graph_two),
                ]
            )
            (trace_dir / "trace.log").write_text(log_text, encoding="utf-8")

            result = ingest_trace_dir("run-2", trace_dir, artifacts_dir)
            keys = [
                artifact.match_key
                for artifact in result.artifacts
                if not artifact.family.startswith("report:")
            ]
            self.assertEqual(keys, ["dynamo_output_graph@1", "dynamo_output_graph@2"])

    def test_cli_wrapper_records_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "emit_trace.py"
            script_path.write_text(
                textwrap.dedent(
                    """
                    import hashlib
                    import json
                    import os
                    from pathlib import Path

                    payload = "graph():\\n    %x = placeholder[target=x]\\n    return x"
                    digest = hashlib.md5(payload.encode("utf-8")).hexdigest()
                    trace = Path(os.environ["TORCH_TRACE"])
                    trace.mkdir(parents=True, exist_ok=True)
                    metadata = json.dumps(
                        {
                            "dynamo_output_graph": {},
                            "frame_id": 0,
                            "frame_compile_id": 0,
                            "attempt": 0,
                            "has_payload": digest,
                        }
                    )
                    header = "V0401 12:00:00.000001 123 torch/_logging/structured.py:27] " + metadata + "\\n"
                    payload_block = "".join(f"\\t{line}\\n" for line in payload.splitlines())
                    (trace / "trace.log").write_text(header + payload_block, encoding="utf-8")
                    """
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                with mock.patch("tlhub.cli.ensure_daemon_running", return_value="http://127.0.0.1:9999"):
                    exit_code = cli.main([sys.executable, str(script_path)])

                self.assertEqual(exit_code, 0)

                repo = Repository(get_paths())
                runs = repo.list_runs()
                self.assertEqual(len(runs), 1)
                run = runs[0]
                self.assertEqual(run["status"], "finished")
                self.assertGreaterEqual(run["artifact_count"], 3)
                self.assertEqual(run["log_count"], 1)

                artifacts = repo.list_artifacts(run["id"])
                primary_artifacts = [
                    artifact for artifact in artifacts if not artifact["family"].startswith("report:")
                ]
                self.assertEqual(len(primary_artifacts), 1)
                artifact_path = Path(tmpdir) / "runs" / run["id"] / "artifacts" / primary_artifacts[0]["relative_path"]
                self.assertTrue(artifact_path.exists())
                self.assertIn("placeholder[target=x]", artifact_path.read_text(encoding="utf-8"))
                manifest = json.loads(
                    (Path(tmpdir) / "runs" / run["id"] / "run_manifest.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(manifest["compile_count"], 1)
                self.assertEqual(len(manifest["compiles"]), 1)

    def test_cli_without_command_opens_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                with mock.patch(
                    "tlhub.cli.ensure_daemon_running",
                    return_value="http://127.0.0.1:9999",
                ) as ensure_running:
                    with mock.patch("webbrowser.open") as open_browser:
                        exit_code = cli.main([])

                self.assertEqual(exit_code, 0)
                ensure_running.assert_called_once()
                open_browser.assert_called_once_with("http://127.0.0.1:9999", new=2)

    def test_ensure_daemon_running_rewrites_with_base_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                os.environ,
                {
                    "TLHUB_HOME": tmpdir,
                    BASE_URL_ENV: "https://proxy.example.com/reviewer?via=ssh",
                },
                clear=False,
            ):
                paths = get_paths()
                paths.daemon_version_path.parent.mkdir(parents=True, exist_ok=True)
                paths.daemon_version_path.write_text(cli.__version__, encoding="utf-8")
                with mock.patch(
                    "tlhub.cli.daemon_status",
                    return_value=(True, "http://127.0.0.1:9345"),
                ):
                    url = cli.ensure_daemon_running(paths, preferred_port=9234)

        self.assertEqual(url, "https://proxy.example.com:9345/reviewer?via=ssh")

    def test_ensure_daemon_running_restarts_mismatched_daemon_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                paths.daemon_pid_path.parent.mkdir(parents=True, exist_ok=True)
                paths.daemon_pid_path.write_text("4321", encoding="utf-8")
                paths.daemon_version_path.write_text("0.0.0", encoding="utf-8")

                with mock.patch(
                    "tlhub.cli.daemon_status",
                    return_value=(True, "http://127.0.0.1:9345"),
                ):
                    with mock.patch("tlhub.cli.process_exists", return_value=True):
                        with mock.patch("tlhub.cli.stop_daemon", return_value=True) as stop_daemon:
                            with mock.patch(
                                "tlhub.cli.start_daemon",
                                return_value=cli.DaemonStartResult(url="http://127.0.0.1:9456"),
                            ) as start_daemon:
                                url = cli.ensure_daemon_running(paths, preferred_port=9234)

        self.assertEqual(url, "http://127.0.0.1:9456")
        stop_daemon.assert_called_once_with(paths)
        start_daemon.assert_called_once_with(paths, preferred_port=9234)

    def test_ensure_daemon_running_restarts_daemon_without_version_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                paths.daemon_pid_path.parent.mkdir(parents=True, exist_ok=True)
                paths.daemon_pid_path.write_text("4321", encoding="utf-8")

                with mock.patch(
                    "tlhub.cli.daemon_status",
                    return_value=(True, "http://127.0.0.1:9345"),
                ):
                    with mock.patch("tlhub.cli.process_exists", return_value=True):
                        with mock.patch("tlhub.cli.stop_daemon", return_value=True) as stop_daemon:
                            with mock.patch(
                                "tlhub.cli.start_daemon",
                                return_value=cli.DaemonStartResult(url="http://127.0.0.1:9456"),
                            ) as start_daemon:
                                url = cli.ensure_daemon_running(paths, preferred_port=9234)

        self.assertEqual(url, "http://127.0.0.1:9456")
        stop_daemon.assert_called_once_with(paths)
        start_daemon.assert_called_once_with(paths, preferred_port=9234)

    def test_ensure_daemon_running_includes_daemon_log_tail_on_start_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                paths.daemon_log_path.parent.mkdir(parents=True, exist_ok=True)
                paths.daemon_log_path.write_text(
                    "Traceback (most recent call last):\nRuntimeError: bind failed\n",
                    encoding="utf-8",
                )
                with mock.patch("tlhub.cli.daemon_status", return_value=(False, None)):
                    with mock.patch(
                        "tlhub.cli.start_daemon",
                        return_value=cli.DaemonStartResult(error="daemon process exited with code 1"),
                    ):
                        with self.assertRaises(SystemExit) as raised:
                            cli.ensure_daemon_running(paths, preferred_port=9234)

        message = str(raised.exception)
        self.assertIn("failed to start tlhub daemon: daemon process exited with code 1", message)
        self.assertIn(str(paths.daemon_log_path), message)
        self.assertIn("RuntimeError: bind failed", message)

    def test_check_health_ignores_proxy_environment_for_local_server(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)
                with run_http_server(paths, repo) as httpd:
                    host, port = httpd.server_address
                    with mock.patch.dict(
                        os.environ,
                        {
                            "HTTP_PROXY": "http://127.0.0.1:9",
                            "http_proxy": "http://127.0.0.1:9",
                            "HTTPS_PROXY": "http://127.0.0.1:9",
                            "https_proxy": "http://127.0.0.1:9",
                            "NO_PROXY": "",
                            "no_proxy": "",
                        },
                        clear=False,
                    ):
                        self.assertTrue(cli.check_health(f"http://{host}:{port}"))

    def test_cli_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                with mock.patch("tlhub.cli.stop_daemon", return_value=True) as stop_daemon:
                    exit_code = cli.main(["--stop"])

                self.assertEqual(exit_code, 0)
                stop_daemon.assert_called_once()

    def test_provenance_fixture_renders_panel_and_detail(self) -> None:
        fixture = Path("/tmp/tlparse-upstream/tests/inputs/inductor_provenance_aot_log.txt")
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)
                run_id = "prov-run"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                stage_fixture(trace_dir, fixture)
                index_run(repo, run_id, trace_dir, artifacts_dir)

                run_html = server.render_run_detail(repo, paths, run_id)
                self.assertIn("Provenance tracking", run_html)
                provenance_groups = build_provenance_groups(
                    [
                        artifact
                        for artifact in repo.list_artifacts(run_id)
                        if not artifact["family"].startswith("report:")
                    ]
                )
                self.assertGreaterEqual(len(provenance_groups), 1)

                detail_html = server.render_provenance_detail(
                    repo,
                    paths,
                    run_id,
                    provenance_groups[0]["id"],
                )
                self.assertIn("Pre-grad graph", detail_html)
                self.assertIn("Post-grad graph", detail_html)
                self.assertIn("Generated code", detail_html)
                self.assertIn("provenanceMappings", detail_html)

    def test_vllm_fixture_renders_subgraph_artifacts(self) -> None:
        fixture = Path("/tmp/tlparse-upstream/tests/inputs/vllm_sample.log")
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)
                run_id = "vllm-run"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                stage_fixture(trace_dir, fixture)
                index_run(repo, run_id, trace_dir, artifacts_dir)

                html = server.render_run_detail(repo, paths, run_id)
                self.assertIn("vLLM summary", html)
                self.assertIn("vllm_piecewise_split_graph", html)
                self.assertIn("vllm_submod_0", html)

    def test_export_fixture_renders_guard_detail(self) -> None:
        fixture = Path("/tmp/tlparse-upstream/tests/inputs/export.log")
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)
                run_id = "export-run"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                stage_fixture(trace_dir, fixture)
                index_run(repo, run_id, trace_dir, artifacts_dir)

                run_html = server.render_run_detail(repo, paths, run_id)
                self.assertIn("Export diagnostics", run_html)
                self.assertIn("View exported program", run_html)

                manifest = server.load_run_manifest(paths, run_id)
                assert manifest is not None
                detail_id = manifest["export"]["guard_details"][0]["id"]
                guard_html = server.render_guard_detail(paths, run_id, detail_id)
                self.assertIn("Expression tree", guard_html)
                self.assertIn("Guard detail", guard_html)

    def test_graph_diff_renders_semantic_diff(self) -> None:
        graph_one = textwrap.dedent(
            """
            graph():
                relu: "f32[4, 4]" = torch.ops.aten.relu.default(x);  x = None
                return (relu,)
            """
        ).strip()
        graph_two = textwrap.dedent(
            """
            graph():
                sigmoid: "f32[4, 4]" = torch.ops.aten.sigmoid.default(x);  x = None
                return (sigmoid,)
            """
        ).strip()
        log_one = emit_event(
            '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
            graph_one,
        )
        log_two = emit_event(
            '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
            graph_two,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)

                for run_id, log_text in (("left-run", log_one), ("right-run", log_two)):
                    trace_dir = paths.runs_dir / run_id / "trace"
                    artifacts_dir = paths.runs_dir / run_id / "artifacts"
                    trace_dir.mkdir(parents=True, exist_ok=True)
                    artifacts_dir.mkdir(parents=True, exist_ok=True)
                    (trace_dir / "trace.log").write_text(log_text, encoding="utf-8")
                    index_run(repo, run_id, trace_dir, artifacts_dir)

                left = next(
                    artifact
                    for artifact in repo.list_artifacts("left-run")
                    if artifact["kind"] == "fx_graph" and not artifact["family"].startswith("report:")
                )
                right = next(
                    artifact
                    for artifact in repo.list_artifacts("right-run")
                    if artifact["kind"] == "fx_graph" and not artifact["family"].startswith("report:")
                )

                diff_html = server.render_diff(
                    repo,
                    paths,
                    {"left": [left["id"]], "right": [right["id"]]},
                )
                self.assertIn("Side-by-side diff", diff_html)
                self.assertIn("sxs-delete", diff_html)
                self.assertIn("sxs-insert", diff_html)

    def test_http_routes_cover_provenance_and_diff(self) -> None:
        provenance_fixture = Path("/tmp/tlparse-upstream/tests/inputs/inductor_provenance_aot_log.txt")
        graph_left = emit_event(
            '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
            "graph():\n    relu: \"f32[4, 4]\" = torch.ops.aten.relu.default(x);  x = None\n    return (relu,)",
        )
        graph_right = emit_event(
            '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
            "graph():\n    sigmoid: \"f32[4, 4]\" = torch.ops.aten.sigmoid.default(x);  x = None\n    return (sigmoid,)",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)

                run_id = "prov-http"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                stage_fixture(trace_dir, provenance_fixture)
                index_run(repo, run_id, trace_dir, artifacts_dir)
                provenance_groups = build_provenance_groups(
                    [
                        artifact
                        for artifact in repo.list_artifacts(run_id)
                        if not artifact["family"].startswith("report:")
                    ]
                )
                self.assertGreaterEqual(len(provenance_groups), 1)

                for diff_run, log_text in (("http-left", graph_left), ("http-right", graph_right)):
                    diff_trace_dir = paths.runs_dir / diff_run / "trace"
                    diff_artifacts_dir = paths.runs_dir / diff_run / "artifacts"
                    diff_trace_dir.mkdir(parents=True, exist_ok=True)
                    diff_artifacts_dir.mkdir(parents=True, exist_ok=True)
                    (diff_trace_dir / "trace.log").write_text(log_text, encoding="utf-8")
                    index_run(repo, diff_run, diff_trace_dir, diff_artifacts_dir)

                left = next(
                    artifact
                    for artifact in repo.list_artifacts("http-left")
                    if artifact["kind"] == "fx_graph" and not artifact["family"].startswith("report:")
                )
                right = next(
                    artifact
                    for artifact in repo.list_artifacts("http-right")
                    if artifact["kind"] == "fx_graph" and not artifact["family"].startswith("report:")
                )

                with run_http_server(paths, repo) as httpd:
                    host, port = httpd.server_address

                    conn = http.client.HTTPConnection(host, port)
                    conn.request("GET", f"/runs/{run_id}/provenance/{provenance_groups[0]['id']}")
                    response = conn.getresponse()
                    provenance_html = response.read().decode("utf-8")
                    conn.close()
                    self.assertEqual(response.status, 200)
                    self.assertIn("Generated code", provenance_html)

                    conn = http.client.HTTPConnection(host, port)
                    conn.request("GET", f"/diff?left={left['id']}&right={right['id']}")
                    response = conn.getresponse()
                    diff_html = response.read().decode("utf-8")
                    conn.close()
                    self.assertEqual(response.status, 200)
                    self.assertIn("Side-by-side diff", diff_html)

    def test_http_post_delete_run_removes_repo_entry_and_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"TLHUB_HOME": tmpdir}, clear=False):
                paths = get_paths()
                repo = Repository(paths)
                run_id = "delete-http"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)

                log_text = emit_event(
                    '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
                    "graph():\n    %x = placeholder[target=x]\n    return x",
                )
                (trace_dir / "trace.log").write_text(log_text, encoding="utf-8")
                index_run(repo, run_id, trace_dir, artifacts_dir)
                self.assertIsNotNone(repo.get_run(run_id))
                self.assertTrue((paths.runs_dir / run_id).exists())

                with run_http_server(paths, repo) as httpd:
                    host, port = httpd.server_address
                    conn = http.client.HTTPConnection(host, port)
                    conn.request("POST", f"/runs/{run_id}/delete")
                    response = conn.getresponse()
                    response.read()
                    conn.close()

                self.assertEqual(response.status, 303)
                self.assertEqual(response.getheader("Location"), "/")
                self.assertIsNone(repo.get_run(run_id))
                self.assertFalse((paths.runs_dir / run_id).exists())

    def test_http_routes_support_base_url_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                os.environ,
                {
                    "TLHUB_HOME": tmpdir,
                    BASE_URL_ENV: "https://proxy.example.com/reviewer?via=ssh",
                },
                clear=False,
            ):
                paths = get_paths()
                repo = Repository(paths)
                run_id = "prefix-run"
                trace_dir = paths.runs_dir / run_id / "trace"
                artifacts_dir = paths.runs_dir / run_id / "artifacts"
                trace_dir.mkdir(parents=True, exist_ok=True)
                artifacts_dir.mkdir(parents=True, exist_ok=True)

                log_text = emit_event(
                    '"dynamo_output_graph": {}, "frame_id": 0, "frame_compile_id": 0, "attempt": 0',
                    "graph():\n    %x = placeholder[target=x]\n    return x",
                )
                (trace_dir / "trace.log").write_text(log_text, encoding="utf-8")
                index_run(repo, run_id, trace_dir, artifacts_dir)

                with run_http_server(paths, repo) as httpd:
                    host, port = httpd.server_address

                    conn = http.client.HTTPConnection(host, port)
                    conn.request("GET", "/reviewer/")
                    response = conn.getresponse()
                    dashboard_html = response.read().decode("utf-8")
                    conn.close()
                    self.assertEqual(response.status, 200)
                    self.assertIn("/reviewer/compare?via=ssh", dashboard_html)
                    self.assertIn("/reviewer/runs/prefix-run?via=ssh", dashboard_html)

                    conn = http.client.HTTPConnection(host, port)
                    conn.request("GET", "/reviewer/runs/prefix-run")
                    response = conn.getresponse()
                    run_html = response.read().decode("utf-8")
                    conn.close()
                    self.assertEqual(response.status, 200)
                    self.assertIn("Run prefix-run", run_html)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import difflib
import html
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import signal
import threading
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlparse

from tlhub.config import DEFAULT_HOST, TLHubPaths, ensure_layout
from tlhub.database import Repository
from tlhub.view_helpers import (
    build_fx_graph_diff,
    build_json_diff,
    build_provenance_groups,
    build_provenance_line_mappings,
)


APP_CSS = """
:root {
  --bg: #f4efe6;
  --bg-strong: #fdf9f2;
  --panel: rgba(255, 250, 242, 0.92);
  --panel-strong: #fffdf8;
  --line: rgba(93, 60, 32, 0.14);
  --text: #23160d;
  --muted: #6f5a47;
  --accent: #b6541f;
  --accent-soft: #f2d1bb;
  --ok: #265c43;
  --warn: #996d00;
  --bad: #8f2f1d;
  --mono: "Iosevka", "JetBrains Mono", "SFMono-Regular", monospace;
  --sans: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  font-family: var(--sans);
  background:
    radial-gradient(circle at top left, rgba(230, 177, 132, 0.32), transparent 28rem),
    radial-gradient(circle at bottom right, rgba(178, 92, 31, 0.12), transparent 24rem),
    linear-gradient(180deg, #fbf7f0 0%, var(--bg) 100%);
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
code, pre, .mono { font-family: var(--mono); }
header.shell {
  padding: 2rem 2rem 1rem;
  border-bottom: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(255,255,255,0.85), rgba(255,255,255,0.55));
  backdrop-filter: blur(8px);
}
.title {
  display: flex;
  justify-content: space-between;
  align-items: end;
  gap: 1rem;
}
.title h1 {
  margin: 0;
  font-size: clamp(1.7rem, 2vw, 2.3rem);
  letter-spacing: -0.04em;
}
.title p {
  margin: 0.35rem 0 0;
  color: var(--muted);
  max-width: 70ch;
}
main {
  padding: 1.5rem 2rem 2rem;
}
.stack {
  display: grid;
  gap: 1rem;
}
.grid-2 {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(20rem, 1fr));
  gap: 1rem;
}
.panel {
  border: 1px solid var(--line);
  border-radius: 18px;
  background: var(--panel);
  box-shadow: 0 10px 30px rgba(73, 45, 20, 0.06);
  padding: 1rem 1.1rem;
}
.panel h2, .panel h3 {
  margin: 0 0 0.8rem;
  font-size: 1rem;
  letter-spacing: -0.02em;
}
.kpis {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(9rem, 1fr));
  gap: 0.75rem;
}
.kpi {
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 0.8rem 0.9rem;
  background: rgba(255, 255, 255, 0.55);
}
.kpi .label {
  display: block;
  font-size: 0.78rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
.kpi .value {
  display: block;
  margin-top: 0.25rem;
  font-size: 1rem;
  font-weight: 600;
}
table.data, table.source {
  width: 100%;
  border-collapse: collapse;
}
table.data th,
table.data td,
table.source td {
  border-top: 1px solid var(--line);
  vertical-align: top;
  text-align: left;
  padding: 0.65rem 0.55rem;
  font-size: 0.94rem;
}
table.data th {
  border-top: none;
  color: var(--muted);
  font-weight: 600;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
table.source {
  border: 1px solid var(--line);
  border-radius: 16px;
  overflow: hidden;
  background: #fffdf9;
}
table.source td.ln {
  width: 1%;
  white-space: nowrap;
  text-align: right;
  color: var(--muted);
  background: rgba(191, 153, 121, 0.13);
}
table.source td.ln a {
  color: inherit;
  text-decoration: none;
}
table.source td.ln a:hover {
  text-decoration: underline;
}
table.source td.code {
  width: 99%;
  overflow-x: auto;
}
table.source code {
  white-space: pre;
}
table.source tr:target td {
  background: rgba(182, 84, 31, 0.12);
}
.pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.22rem 0.6rem;
  font-size: 0.76rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.pill.running { background: rgba(38, 92, 67, 0.14); color: var(--ok); }
.pill.finished { background: rgba(182, 84, 31, 0.13); color: var(--accent); }
.pill.failed { background: rgba(143, 47, 29, 0.14); color: var(--bad); }
.pill.same { background: rgba(38, 92, 67, 0.14); color: var(--ok); }
.pill.changed { background: rgba(153, 109, 0, 0.14); color: var(--warn); }
.pill.missing { background: rgba(111, 90, 71, 0.12); color: var(--muted); }
.toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  align-items: center;
}
form.inline, .toolbar form {
  display: inline-flex;
  gap: 0.55rem;
  align-items: center;
  flex-wrap: wrap;
}
button, select, input {
  font: inherit;
  border-radius: 10px;
  border: 1px solid var(--line);
  padding: 0.55rem 0.75rem;
  background: var(--panel-strong);
  color: var(--text);
}
button {
  cursor: pointer;
  background: linear-gradient(180deg, #fff9ef, #f2e4d6);
}
button.danger {
  color: var(--bad);
}
.muted { color: var(--muted); }
.actions {
  display: flex;
  gap: 0.55rem;
  align-items: center;
  flex-wrap: wrap;
}
.summary-list {
  display: grid;
  gap: 0.35rem;
  font-size: 0.93rem;
}
.summary-list div {
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  border-top: 1px solid var(--line);
  padding-top: 0.35rem;
}
.summary-list div:first-child {
  border-top: none;
  padding-top: 0;
}
.stack-tree {
  font-family: var(--mono);
  font-size: 0.9rem;
}
.stack-tree ul {
  list-style: none;
  padding-left: 1.1rem;
  margin: 0.35rem 0;
}
.stack-tree li {
  margin: 0.2rem 0;
}
.stack-frame {
  display: inline-block;
  color: var(--text);
}
.status-ok { background: rgba(38, 92, 67, 0.12); color: var(--ok); }
.status-break { background: rgba(153, 109, 0, 0.14); color: var(--warn); }
.status-empty { background: rgba(111, 90, 71, 0.12); color: var(--muted); }
.status-error { background: rgba(143, 47, 29, 0.14); color: var(--bad); }
.status-missing { background: rgba(111, 90, 71, 0.12); color: var(--muted); }
.mono-list {
  margin: 0;
  padding-left: 1.1rem;
}
.mono-list li {
  margin: 0.25rem 0;
  font-family: var(--mono);
}
.section-copy {
  color: var(--muted);
  max-width: 72ch;
  margin: 0.2rem 0 0.9rem;
}
.frame-block {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.55);
  padding: 0.8rem 0.9rem;
}
.frame-block + .frame-block {
  margin-top: 0.65rem;
}
.frame-line {
  font-family: var(--mono);
  font-size: 0.9rem;
}
.frame-loc {
  color: var(--muted);
  font-family: var(--mono);
  font-size: 0.83rem;
  margin-top: 0.25rem;
}
.text-block {
  white-space: pre-wrap;
  font-family: var(--mono);
  font-size: 0.88rem;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: #fffdf9;
  padding: 0.9rem;
}
.mini-note {
  font-size: 0.84rem;
  color: var(--muted);
}
.diff-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: #fffdf9;
}
.diff {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--mono);
  font-size: 0.88rem;
}
.diff td, .diff th {
  padding: 0.25rem 0.45rem;
}
.diff_header {
  background: rgba(191, 153, 121, 0.15);
}
.diff_add { background: rgba(38, 92, 67, 0.12); }
.diff_sub { background: rgba(143, 47, 29, 0.12); }
.diff_chg { background: rgba(153, 109, 0, 0.12); }
.empty {
  padding: 2rem;
  border: 1px dashed var(--line);
  border-radius: 16px;
  color: var(--muted);
  background: rgba(255,255,255,0.45);
}
details.unified summary {
  cursor: pointer;
  font-weight: 600;
}
pre.unified {
  padding: 1rem;
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: #fffdf9;
}
@media (max-width: 720px) {
  header.shell, main { padding-left: 1rem; padding-right: 1rem; }
  .title { flex-direction: column; align-items: start; }
}
"""


PROVENANCE_STYLE = """
.prov-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(22rem, 1fr));
  gap: 1rem;
}
.prov-shell {
  border: 1px solid var(--line);
  border-radius: 18px;
  background: #fffdf9;
  overflow: hidden;
}
.prov-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  padding: 0.85rem 1rem;
  border-bottom: 1px solid var(--line);
  background: rgba(191, 153, 121, 0.08);
}
.prov-head h2 {
  margin: 0;
  font-size: 0.98rem;
}
.prov-surface {
  max-height: 68vh;
  overflow: auto;
}
.prov-line {
  display: grid;
  grid-template-columns: 4.2rem 1fr;
  gap: 0.8rem;
  padding: 0.1rem 0.9rem 0.1rem 0.2rem;
  border-top: 1px solid rgba(93, 60, 32, 0.08);
  scroll-margin-top: 6rem;
}
.prov-line:first-child {
  border-top: none;
}
.prov-line code {
  display: block;
  white-space: pre;
  overflow-x: auto;
  padding: 0.12rem 0;
}
.prov-line.active {
  background: rgba(182, 84, 31, 0.12);
}
.prov-line.related {
  background: rgba(38, 92, 67, 0.1);
}
.prov-line[data-linked="1"] code {
  border-left: 3px solid rgba(182, 84, 31, 0.45);
  padding-left: 0.6rem;
}
.prov-ln {
  display: inline-block;
  min-width: 3.2rem;
  padding: 0.12rem 0.5rem;
  text-align: right;
  color: var(--muted);
  text-decoration: none;
  font-family: var(--mono);
  background: rgba(191, 153, 121, 0.13);
  border-radius: 8px;
}
.prov-ln:hover {
  text-decoration: underline;
}
"""


PROVENANCE_SCRIPT = """
(() => {
  const mappingsEl = document.getElementById("provenanceMappings");
  if (!mappingsEl) {
    return;
  }
  let mappings = {};
  try {
    mappings = JSON.parse(mappingsEl.textContent || "{}");
  } catch (_error) {
    mappings = {};
  }

  const selector = (pane, line) =>
    `.prov-line[data-pane="${pane}"][data-line="${String(line)}"]`;

  function clearHighlights() {
    document.querySelectorAll(".prov-line").forEach((node) => {
      node.classList.remove("active", "related");
    });
  }

  function markTargets(pane, lines, scroll) {
    for (const line of lines || []) {
      const row = document.querySelector(selector(pane, line));
      if (!row) {
        continue;
      }
      row.classList.add("related");
      if (scroll) {
        row.scrollIntoView({ block: "center", inline: "nearest", behavior: "smooth" });
        scroll = false;
      }
    }
  }

  function highlight(pane, line, scroll) {
    clearHighlights();
    const origin = document.querySelector(selector(pane, line));
    if (origin) {
      origin.classList.add("active");
    }

    const key = String(line);
    if (pane === "pre") {
      markTargets("post", mappings.preToPost?.[key], scroll);
      return;
    }
    if (pane === "post") {
      markTargets("pre", mappings.postToPre?.[key], scroll);
      markTargets("code", mappings.postToCode?.[key], scroll);
      return;
    }
    markTargets("post", mappings.codeToPost?.[key], scroll);
  }

  document.querySelectorAll(".prov-line").forEach((node) => {
    node.addEventListener("mouseenter", () => {
      highlight(node.dataset.pane, node.dataset.line, false);
    });
    node.addEventListener("click", () => {
      highlight(node.dataset.pane, node.dataset.line, true);
    });
  });
})();
"""


def run_daemon(paths: TLHubPaths, *, host: str = DEFAULT_HOST, port: int = 0) -> None:
    ensure_layout(paths)
    repo = Repository(paths)
    handler = make_handler(paths, repo)
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    actual_port = int(server.server_address[1])

    def stop_server(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGINT, stop_server)

    paths.daemon_pid_path.write_text(str(os.getpid()), encoding="utf-8")
    paths.daemon_port_path.write_text(str(actual_port), encoding="utf-8")

    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        for path in (paths.daemon_pid_path, paths.daemon_port_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def make_handler(paths: TLHubPaths, repo: Repository) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "tlhub/0.1.0"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            path = parsed.path

            if path == "/healthz":
                self.respond_text("ok")
                return
            if path == "/":
                self.respond_html(render_dashboard(repo, paths))
                return
            if path == "/compare":
                self.respond_html(render_compare(repo, params))
                return
            if path == "/diff":
                self.respond_html(render_diff(repo, paths, params))
                return

            parts = [part for part in path.split("/") if part]
            if len(parts) == 2 and parts[0] == "runs":
                self.respond_html(render_run_detail(repo, paths, parts[1]))
                return
            if len(parts) == 2 and parts[0] == "artifacts":
                self.respond_html(render_artifact(repo, paths, parts[1]))
                return
            if len(parts) == 4 and parts[0] == "runs" and parts[2] == "compiles":
                self.respond_html(render_compile_detail(repo, paths, parts[1], parts[3]))
                return
            if len(parts) == 4 and parts[0] == "runs" and parts[2] == "provenance":
                self.respond_html(render_provenance_detail(repo, paths, parts[1], parts[3]))
                return
            if len(parts) == 4 and parts[0] == "runs" and parts[2] == "guards":
                self.respond_html(render_guard_detail(paths, parts[1], parts[3]))
                return

            self.respond_error(HTTPStatus.NOT_FOUND, "Page not found")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) == 3 and parts[0] == "runs" and parts[2] == "delete":
                run_id = parts[1]
                repo.delete_run(run_id)
                shutil.rmtree(paths.runs_dir / run_id, ignore_errors=True)
                self.redirect("/")
                return
            self.respond_error(HTTPStatus.NOT_FOUND, "Action not found")

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

        def respond_html(self, body: str, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def respond_text(self, body: str, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def respond_error(self, status: HTTPStatus, message: str) -> None:
            self.respond_html(page("Error", f"<div class='empty'>{escape(message)}</div>"), status=status)

        def redirect(self, location: str) -> None:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            self.end_headers()

    return Handler


def render_dashboard(repo: Repository, paths: TLHubPaths) -> str:
    runs = repo.list_runs()
    compare_form = render_compare_picker(runs)
    if not runs:
        table = "<div class='empty'>No runs captured yet. Prefix a command with <code>tlhub</code> to start collecting traces.</div>"
    else:
        rows = "".join(render_run_row(run, load_run_manifest(paths, run["id"])) for run in runs)
        table = f"""
        <div class="panel">
          <h2>Runs</h2>
          <table class="data">
            <thead>
              <tr>
                <th>ID</th>
                <th>Status</th>
                <th>Command</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Artifacts</th>
                <th>Compiles</th>
                <th>Ranks</th>
                <th>Exit</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
        """

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>tlhub</h1>
          <p>Browse raw <code>TORCH_TRACE</code> runs, inspect compile summaries, failures, raw JSONL, export diagnostics, and multi-rank artifacts, then diff any extracted output across runs.</p>
        </div>
      </div>
    </header>
    <main class="stack">
      {compare_form}
      {table}
    </main>
    """
    return page("tlhub", body)


def render_run_detail(repo: Repository, paths: TLHubPaths, run_id: str) -> str:
    run = repo.get_run(run_id)
    if run is None:
        return page("Run not found", f"<div class='empty'>Run <code>{escape(run_id)}</code> was not found.</div>")
    manifest = load_run_manifest(paths, run_id)
    if manifest is None:
        return page("Run not found", f"<div class='empty'>Run manifest for <code>{escape(run_id)}</code> is missing.</div>")

    runs = repo.list_runs()
    compare_form = render_compare_picker(runs, left_run=run_id)
    artifacts = repo.list_artifacts(run_id)
    artifact_by_id = {artifact["id"]: artifact for artifact in artifacts}
    report_artifacts = [
        artifact_by_id[artifact_id]
        for artifact_id in manifest.get("report_artifact_ids", [])
        if artifact_id in artifact_by_id
    ]
    primary_artifacts = [artifact for artifact in artifacts if artifact["id"] not in manifest.get("report_artifact_ids", [])]
    provenance_groups = build_provenance_groups(primary_artifacts)
    compile_table = render_compile_table(run_id, manifest.get("compiles", []))
    failures = render_failures_panel(manifest.get("failures_and_restarts", []))
    export_panel = render_export_panel(run_id, manifest.get("export"))
    multirank_panel = render_multi_rank_panel(manifest.get("multi_rank"))
    warnings_panel = render_warnings_panel(manifest.get("warnings", []))
    vllm_panel = render_vllm_panel(manifest.get("vllm"))
    provenance_panel = render_provenance_panel(run_id, provenance_groups, artifact_by_id)
    stack_trie = render_stack_trie(run_id, manifest.get("compiles", []))
    unknown_stacks = render_unknown_stacks(manifest.get("unknown_stacks", []))
    report_panel = render_report_artifacts(report_artifacts)

    artifact_table = (
        "<div class='empty'>This run produced no indexed artifacts.</div>"
        if not primary_artifacts
        else f"""
        <div class="panel">
          <h2>Artifacts</h2>
          <p class="section-copy">These are the extracted per-event payloads and code or graph dumps. Synthetic reports like <code>raw.jsonl</code> and multi-rank analyses are listed separately above.</p>
          <table class="data">
            <thead>
              <tr>
                <th>Artifact</th>
                <th>Match key</th>
                <th>Kind</th>
                <th>Origin</th>
                <th>Summary</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {''.join(render_artifact_row(artifact) for artifact in primary_artifacts)}
            </tbody>
          </table>
        </div>
        """
    )

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>Run {escape(run['id'])}</h1>
          <p class="mono">{escape(run['command_display'])}</p>
        </div>
        <div class="actions">
          <a href="/">Back</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <div class="grid-2">
        <div class="panel">
          <h2>Run metadata</h2>
          <div class="kpis">
            <div class="kpi"><span class="label">Status</span><span class="value">{render_status(run['status'])}</span></div>
            <div class="kpi"><span class="label">Started</span><span class="value">{escape(run['started_at'])}</span></div>
            <div class="kpi"><span class="label">Duration</span><span class="value">{escape(format_duration(run['duration_ms']))}</span></div>
            <div class="kpi"><span class="label">Artifacts</span><span class="value">{run['artifact_count']}</span></div>
            <div class="kpi"><span class="label">Compiles</span><span class="value">{manifest.get('compile_count', 0)}</span></div>
            <div class="kpi"><span class="label">Ranks</span><span class="value">{format_rank_count(manifest.get('multi_rank'))}</span></div>
            <div class="kpi"><span class="label">Logs</span><span class="value">{run['log_count']}</span></div>
            <div class="kpi"><span class="label">Exit</span><span class="value">{'' if run['exit_code'] is None else run['exit_code']}</span></div>
          </div>
          <div class="summary-list" style="margin-top:1rem;">
            <div><span class="muted">Trace dir</span><span class="mono">{escape(run['trace_dir'])}</span></div>
            <div><span class="muted">CWD</span><span class="mono">{escape(run['cwd'])}</span></div>
            <div><span class="muted">Host</span><span class="mono">{escape(run['hostname'])}</span></div>
            <div><span class="muted">Warnings</span><span class="mono">{len(manifest.get('warnings', []))}</span></div>
          </div>
        </div>
        {compare_form}
      </div>
      {report_panel}
      {compile_table}
      {warnings_panel}
      {failures}
      {export_panel}
      {multirank_panel}
      {vllm_panel}
      {provenance_panel}
      {stack_trie}
      {unknown_stacks}
      {artifact_table}
    </main>
    """
    return page(f"Run {run_id}", body)


def render_artifact(repo: Repository, paths: TLHubPaths, artifact_id: str) -> str:
    artifact = repo.get_artifact(artifact_id)
    if artifact is None:
        return page("Artifact not found", f"<div class='empty'>Artifact <code>{escape(artifact_id)}</code> was not found.</div>")

    run = repo.get_run(artifact["run_id"])
    assert run is not None
    content = read_artifact_text(paths, artifact)
    summary = render_summary_card(artifact["summary"])
    source = render_source_block(content)
    compare_link = "/compare?" + urlencode({"left_run": artifact["run_id"], "family": artifact["family"]})

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(artifact['title'])}</h1>
          <p><a href="/runs/{quote(run['id'])}">Run {escape(run['id'])}</a> | <span class="mono">{escape(artifact['relative_path'])}</span></p>
        </div>
        <div class="actions">
          <a href="{compare_link}">Compare this family</a>
          <a href="/runs/{quote(run['id'])}">Back to run</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <div class="grid-2">
        <div class="panel">
          <h2>Artifact metadata</h2>
          <div class="summary-list">
            <div><span class="muted">Match key</span><span class="mono">{escape(artifact['match_key'])}</span></div>
            <div><span class="muted">Kind</span><span>{escape(artifact['kind'])}</span></div>
            <div><span class="muted">Event</span><span>{escape(artifact['event_type'])}</span></div>
            <div><span class="muted">Compile</span><span class="mono">{escape(artifact['compile_id'] or 'n/a')}</span></div>
            <div><span class="muted">Origin</span><span class="mono">{escape(f"{artifact['log_file']}:{artifact['line_no']}")}</span></div>
            <div><span class="muted">SHA</span><span class="mono">{escape(artifact['sha256'][:16])}</span></div>
          </div>
        </div>
        <div class="panel">
          <h2>Summary</h2>
          {summary}
        </div>
      </div>
      <div class="panel">
        <h2>Content</h2>
        {source}
      </div>
    </main>
    """
    return page(artifact["title"], body)


def render_compare(repo: Repository, params: dict[str, list[str]]) -> str:
    runs = repo.list_runs()
    left_run_id = first_param(params, "left_run")
    right_run_id = first_param(params, "right_run")
    family_filter = first_param(params, "family") or ""

    compare_form = render_compare_picker(runs, left_run=left_run_id, right_run=right_run_id, family=family_filter)

    content = ""
    if left_run_id and right_run_id:
        left_run = repo.get_run(left_run_id)
        right_run = repo.get_run(right_run_id)
        if left_run is None or right_run is None:
            content = "<div class='empty'>One of the requested runs no longer exists.</div>"
        else:
            left_artifacts = repo.list_artifacts(left_run_id)
            right_artifacts = repo.list_artifacts(right_run_id)
            if family_filter:
                left_artifacts = [artifact for artifact in left_artifacts if family_filter in artifact["family"] or family_filter in artifact["title"]]
                right_artifacts = [artifact for artifact in right_artifacts if family_filter in artifact["family"] or family_filter in artifact["title"]]

            picker = render_arbitrary_diff_picker(left_artifacts, right_artifacts)
            matches = render_match_table(left_artifacts, right_artifacts)
            content = f"""
            <div class="grid-2">
              {picker}
              <div class="panel">
                <h2>Run pair</h2>
                <div class="summary-list">
                  <div><span class="muted">Left</span><span class="mono">{escape(left_run['id'])}</span></div>
                  <div><span class="muted">Right</span><span class="mono">{escape(right_run['id'])}</span></div>
                  <div><span class="muted">Filter</span><span class="mono">{escape(family_filter or 'none')}</span></div>
                </div>
              </div>
            </div>
            {matches}
            """

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>Compare Runs</h1>
          <p>Match artifacts by family and occurrence order, or pick any two artifacts manually when the pairing is not obvious.</p>
        </div>
        <div class="actions">
          <a href="/">Back</a>
        </div>
      </div>
    </header>
    <main class="stack">
      {compare_form}
      {content if content else "<div class='empty'>Choose two runs to line up their artifacts.</div>"}
    </main>
    """
    return page("Compare runs", body)


def render_diff(repo: Repository, paths: TLHubPaths, params: dict[str, list[str]]) -> str:
    left_id = first_param(params, "left")
    right_id = first_param(params, "right")
    if not left_id or not right_id:
        return page("Diff", "<div class='empty'>Provide both <code>left</code> and <code>right</code> artifact ids.</div>")

    left = repo.get_artifact(left_id)
    right = repo.get_artifact(right_id)
    if left is None or right is None:
        return page("Diff", "<div class='empty'>One of the requested artifacts no longer exists.</div>")

    left_text = read_artifact_text(paths, left)
    right_text = read_artifact_text(paths, right)

    diff_table = difflib.HtmlDiff(wrapcolumn=120).make_table(
        left_text.splitlines(),
        right_text.splitlines(),
        fromdesc=html.escape(left["title"]),
        todesc=html.escape(right["title"]),
        context=True,
        numlines=4,
    )
    unified = "\n".join(
        difflib.unified_diff(
            left_text.splitlines(),
            right_text.splitlines(),
            fromfile=left["title"],
            tofile=right["title"],
            n=4,
            lineterm="",
        )
    )
    summary = render_diff_summary(left["summary"], right["summary"])
    semantic_diff = render_semantic_diff(left, right, left_text, right_text)
    compare_query = {
        "left_run": left["run_id"],
        "right_run": right["run_id"],
    }
    if left["family"] == right["family"]:
        compare_query["family"] = left["family"]

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>Artifact Diff</h1>
          <p><span class="mono">{escape(left['match_key'])}</span> vs <span class="mono">{escape(right['match_key'])}</span></p>
        </div>
        <div class="actions">
          <a href="/compare?{urlencode(compare_query)}">Back to compare</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <div class="grid-2">
        {render_diff_side("Left", left)}
        {render_diff_side("Right", right)}
      </div>
      <div class="panel">
        <h2>Delta summary</h2>
        {summary}
      </div>
      {semantic_diff}
      <div class="panel">
        <h2>Side-by-side diff</h2>
        <div class="diff-wrap">{diff_table}</div>
      </div>
      <div class="panel">
        <details class="unified" open>
          <summary>Unified diff</summary>
          <pre class="unified">{escape(unified or 'No textual diff.')}</pre>
        </details>
      </div>
    </main>
    """
    return page("Diff", body)


def render_compile_detail(
    repo: Repository,
    paths: TLHubPaths,
    run_id: str,
    compile_key: str,
) -> str:
    run = repo.get_run(run_id)
    manifest = load_run_manifest(paths, run_id)
    if run is None or manifest is None:
        return page("Compile not found", "<div class='empty'>Run manifest is missing.</div>")

    compile_entry = next(
        (entry for entry in manifest.get("compiles", []) if entry.get("compile_dir") == compile_key),
        None,
    )
    if compile_entry is None:
        return page("Compile not found", f"<div class='empty'>Compile <code>{escape(compile_key)}</code> was not found.</div>")

    metrics = compile_entry.get("compilation_metrics") or {}
    extra_metrics = {
        key: value
        for key, value in metrics.items()
        if key
        not in {
            "co_name",
            "co_filename",
            "co_firstlineno",
            "cache_size",
            "accumulated_cache_size",
            "guard_count",
            "shape_env_guard_count",
            "graph_op_count",
            "graph_node_count",
            "graph_input_count",
            "start_time",
            "entire_frame_compile_time_s",
            "backend_compile_time_s",
            "inductor_compile_time_s",
            "code_gen_time_s",
            "fail_type",
            "fail_reason",
            "fail_user_frame_filename",
            "fail_user_frame_lineno",
            "non_compliant_ops",
            "compliant_custom_ops",
            "restart_reasons",
            "dynamo_time_before_restart_s",
        }
    }
    guard_links = render_compile_guard_links(run_id, compile_entry, manifest)
    provenance_links = render_compile_provenance_links(
        run_id,
        compile_entry,
        build_provenance_groups(repo.list_artifacts(run_id)),
    )
    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(compile_entry['compile_id'])}</h1>
          <p><a href="/runs/{quote(run_id)}">Run {escape(run_id)}</a> | <span class="mono">{escape(compile_entry['compile_dir'])}</span></p>
        </div>
        <div class="actions">
          <a href="/runs/{quote(run_id)}">Back to run</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <div class="grid-2">
        <div class="panel">
          <h2>Overview</h2>
          <div class="kpis">
            <div class="kpi"><span class="label">Status</span><span class="value">{render_compile_status(compile_entry.get('status', 'missing'))}</span></div>
            <div class="kpi"><span class="label">Rank</span><span class="value">{'' if compile_entry.get('rank') is None else compile_entry['rank']}</span></div>
            <div class="kpi"><span class="label">Artifacts</span><span class="value">{compile_entry.get('artifact_count', 0)}</span></div>
            <div class="kpi"><span class="label">Cache</span><span class="value">{escape(compile_entry.get('cache_status') or 'unknown')}</span></div>
            <div class="kpi"><span class="label">Graph Ops</span><span class="value">{metrics.get('graph_op_count', 'n/a')}</span></div>
            <div class="kpi"><span class="label">Compile Time</span><span class="value">{format_optional_seconds(metrics.get('entire_frame_compile_time_s'))}</span></div>
          </div>
          <div class="summary-list" style="margin-top:1rem;">
            <div><span class="muted">Python frame</span><span class="mono">{escape(str(metrics.get('co_filename') or 'n/a'))}</span></div>
            <div><span class="muted">Function</span><span class="mono">{escape(str(metrics.get('co_name') or 'n/a'))}</span></div>
            <div><span class="muted">Line</span><span class="mono">{escape(str(metrics.get('co_firstlineno') or 'n/a'))}</span></div>
          </div>
        </div>
        <div class="panel">
          <h2>Output files</h2>
          {render_compile_artifacts(compile_entry.get("artifacts", []))}
        </div>
      </div>
      <div class="panel">
        <h2>Stack</h2>
        {render_stack_frames(compile_entry.get("stack", []))}
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>Compile metrics</h2>
          {render_metrics_list({
              "Entire frame compile time (s)": metrics.get("entire_frame_compile_time_s"),
              "Backend compile time (s)": metrics.get("backend_compile_time_s"),
              "Inductor compile time (s)": metrics.get("inductor_compile_time_s"),
              "Codegen time (s)": metrics.get("code_gen_time_s"),
              "Dynamo restart time (s)": metrics.get("dynamo_time_before_restart_s"),
              "Cache size": metrics.get("cache_size"),
              "Accumulated cache size": metrics.get("accumulated_cache_size"),
              "Guard count": metrics.get("guard_count"),
              "Shape env guards": metrics.get("shape_env_guard_count"),
              "Graph ops": metrics.get("graph_op_count"),
              "Graph nodes": metrics.get("graph_node_count"),
              "Graph inputs": metrics.get("graph_input_count"),
          })}
        </div>
        <div class="panel">
          <h2>Restarts and failures</h2>
          {render_compile_failures(metrics)}
          {guard_links}
          {provenance_links}
        </div>
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>Symbolic shape specializations</h2>
          {render_specialization_table(compile_entry.get("symbolic_shape_specializations", []))}
        </div>
        <div class="panel">
          <h2>Guards added fast</h2>
          {render_guard_fast_table(compile_entry.get("guards_added_fast", []))}
        </div>
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>Created symbols</h2>
          {render_symbol_table(compile_entry.get("create_symbols", []), created=True)}
        </div>
        <div class="panel">
          <h2>Unbacked symbols</h2>
          {render_symbol_table(compile_entry.get("unbacked_symbols", []), created=False)}
        </div>
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>Links</h2>
          {render_compile_links(compile_entry.get("links", []))}
        </div>
        <div class="panel">
          <h2>Other metrics</h2>
          {render_json_table(extra_metrics)}
        </div>
      </div>
    </main>
    """
    return page(compile_entry["compile_id"], body)


def render_guard_detail(paths: TLHubPaths, run_id: str, guard_id: str) -> str:
    manifest = load_run_manifest(paths, run_id)
    if manifest is None:
        return page("Guard not found", "<div class='empty'>Run manifest is missing.</div>")
    export = manifest.get("export") or {}
    guard = next(
        (item for item in export.get("guard_details", []) if item.get("id") == guard_id),
        None,
    )
    if guard is None:
        return page("Guard not found", f"<div class='empty'>Guard <code>{escape(guard_id)}</code> was not found.</div>")

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(guard.get('failure_type') or 'Guard detail')}</h1>
          <p><a href="/runs/{quote(run_id)}">Run {escape(run_id)}</a> | <a href="/runs/{quote(run_id)}/compiles/{quote(guard.get('compile_dir') or '')}">{escape(guard.get('compile_id') or 'compile')}</a></p>
        </div>
        <div class="actions">
          <a href="/runs/{quote(run_id)}">Back to run</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <div class="grid-2">
        <div class="panel">
          <h2>Guard</h2>
          <div class="summary-list">
            <div><span class="muted">Expr</span><span class="mono">{escape(str(guard.get('expr') or 'n/a'))}</span></div>
            <div><span class="muted">Result</span><span class="mono">{escape(str(guard.get('result') or 'n/a'))}</span></div>
            <div><span class="muted">Rank</span><span class="mono">{escape(str(guard.get('rank') if guard.get('rank') is not None else 'n/a'))}</span></div>
          </div>
        </div>
        <div class="panel">
          <h2>Sources</h2>
          {render_json_table(guard.get("symbol_to_sources") or {})}
        </div>
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>User stack</h2>
          {render_stack_frames(guard.get("user_stack", []))}
        </div>
        <div class="panel">
          <h2>Framework stack</h2>
          {render_stack_frames(guard.get("framework_stack", []))}
        </div>
      </div>
      <div class="grid-2">
        <div class="panel">
          <h2>Locals</h2>
          <div class="text-block">{escape(json.dumps(guard.get("frame_locals") or {}, indent=2, sort_keys=True))}</div>
        </div>
        <div class="panel">
          <h2>Expression tree</h2>
          {render_expression_tree(guard.get("expression_tree"))}
        </div>
      </div>
    </main>
    """
    return page("Guard detail", body)


def render_provenance_panel(
    run_id: str,
    provenance_groups: list[dict[str, Any]],
    artifact_by_id: dict[str, dict[str, Any]],
) -> str:
    if not provenance_groups:
        return ""
    rows = []
    for group in provenance_groups:
        available = []
        if group.get("pre_grad_artifact_id"):
            available.append("pre-grad")
        if group.get("post_grad_artifact_id"):
            available.append("post-grad")
        if group.get("output_code_artifact_id"):
            available.append("python")
        if group.get("aot_code_artifact_id"):
            available.append("aot")
        mapping_artifact = artifact_by_id.get(group["mapping_artifact_id"])
        origin = (
            f"{mapping_artifact['log_file']}:{mapping_artifact['line_no']}"
            if mapping_artifact
            else "n/a"
        )
        rows.append(
            f"""
            <tr>
              <td><a href="/runs/{quote(run_id)}/provenance/{quote(group['id'])}">{escape(group['label'])}</a></td>
              <td>{'' if group.get('rank') is None else group['rank']}</td>
              <td class="mono">{escape(str(group.get('compile_id') or group.get('compile_dir') or 'n/a'))}</td>
              <td>{escape(", ".join(available) or "mapping only")}</td>
              <td class="mono">{escape(origin)}</td>
            </tr>
            """
        )
    return f"""
    <div class="panel">
      <h2>Provenance tracking</h2>
      <p class="section-copy">This mirrors the dedicated `tlparse` provenance view: pre-grad graph, post-grad graph, and generated code are aligned through node-mapping artifacts so you can inspect how an FX node flows into emitted kernels.</p>
      <table class="data">
        <thead>
          <tr>
            <th>View</th>
            <th>Rank</th>
            <th>Compile</th>
            <th>Artifacts</th>
            <th>Origin</th>
          </tr>
        </thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


def render_provenance_detail(
    repo: Repository,
    paths: TLHubPaths,
    run_id: str,
    provenance_id: str,
) -> str:
    run = repo.get_run(run_id)
    if run is None:
        return page("Provenance not found", "<div class='empty'>Run was not found.</div>")

    artifacts = repo.list_artifacts(run_id)
    primary_artifacts = [artifact for artifact in artifacts if not artifact["family"].startswith("report:")]
    groups = build_provenance_groups(primary_artifacts)
    artifact_by_id = {artifact["id"]: artifact for artifact in primary_artifacts}
    group = next((item for item in groups if item["id"] == provenance_id), None)
    if group is None:
        return page(
            "Provenance not found",
            f"<div class='empty'>Provenance view <code>{escape(provenance_id)}</code> was not found.</div>",
        )

    pre_grad_artifact = artifact_by_id.get(group.get("pre_grad_artifact_id"))
    post_grad_artifact = artifact_by_id.get(group.get("post_grad_artifact_id"))
    output_code_artifact = artifact_by_id.get(group.get("output_code_artifact_id"))
    aot_code_artifact = artifact_by_id.get(group.get("aot_code_artifact_id"))
    mapping_artifact = artifact_by_id.get(group["mapping_artifact_id"])
    extra_artifacts = [
        artifact_by_id[artifact_id]
        for artifact_id in group.get("extra_artifact_ids", [])
        if artifact_id in artifact_by_id
    ]

    pre_grad_text = read_artifact_text(paths, pre_grad_artifact) if pre_grad_artifact else ""
    post_grad_text = read_artifact_text(paths, post_grad_artifact) if post_grad_artifact else ""
    output_code_text = read_artifact_text(paths, output_code_artifact) if output_code_artifact else ""
    aot_code_text = read_artifact_text(paths, aot_code_artifact) if aot_code_artifact else ""
    node_mappings: dict[str, Any] | None = None
    if mapping_artifact is not None:
        try:
            node_mappings = json.loads(read_artifact_text(paths, mapping_artifact))
        except json.JSONDecodeError:
            node_mappings = None

    line_mappings = build_provenance_line_mappings(
        node_mappings,
        pre_grad_text,
        post_grad_text,
        output_code_text,
        aot_code_text,
    )
    code_mode = "python" if output_code_text.strip() else "cpp" if aot_code_text.strip() else "none"
    code_artifact = output_code_artifact if code_mode == "python" else aot_code_artifact
    code_text = output_code_text if code_mode == "python" else aot_code_text
    interactive_mappings = {
        "preToPost": line_mappings.get("preToPost", {}),
        "postToPre": line_mappings.get("postToPre", {}),
        "codeToPost": (
            line_mappings.get("pyCodeToPost", {})
            if code_mode == "python"
            else line_mappings.get("cppCodeToPost", {})
        ),
        "postToCode": (
            line_mappings.get("postToPyCode", {})
            if code_mode == "python"
            else line_mappings.get("postToCppCode", {})
        ),
    }
    mapping_counts = {
        "Pre -> post": len(interactive_mappings["preToPost"]),
        "Post -> pre": len(interactive_mappings["postToPre"]),
        "Code -> post": len(interactive_mappings["codeToPost"]),
        "Post -> code": len(interactive_mappings["postToCode"]),
    }
    alternate_code_link = (
        f"<a href='/artifacts/{quote(aot_code_artifact['id'])}'>Open AOT wrapper artifact</a>"
        if code_mode == "python" and aot_code_artifact is not None
        else (
            f"<a href='/artifacts/{quote(output_code_artifact['id'])}'>Open Python output artifact</a>"
            if code_mode == "cpp" and output_code_artifact is not None
            else ""
        )
    )
    extra_links = (
        "".join(
            f"<li><a href='/artifacts/{quote(artifact['id'])}'>{escape(artifact['title'])}</a></li>"
            for artifact in extra_artifacts
        )
        if extra_artifacts
        else "<li class='muted'>No supplemental provenance artifacts.</li>"
    )

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(group['label'])}</h1>
          <p><a href="/runs/{quote(run_id)}">Run {escape(run_id)}</a> | <span class="mono">{escape(str(group.get('compile_id') or group.get('compile_dir') or provenance_id))}</span></p>
        </div>
        <div class="actions">
          <a href="/runs/{quote(run_id)}">Back to run</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <style>{PROVENANCE_STYLE}</style>
      <script id="provenanceMappings" type="application/json">{json.dumps(interactive_mappings, sort_keys=True).replace("</", "<\\/")}</script>
      <div class="grid-2">
        <div class="panel">
          <h2>Mapping summary</h2>
          {render_metrics_list(mapping_counts)}
          <div class="summary-list" style="margin-top:1rem;">
            <div><span class="muted">Rank</span><span class="mono">{escape(str(group.get('rank') if group.get('rank') is not None else 'n/a'))}</span></div>
            <div><span class="muted">Node mappings</span><span>{render_provenance_artifact_link(mapping_artifact, 'Open artifact')}</span></div>
            <div><span class="muted">Code mode</span><span class="mono">{escape(code_mode)}</span></div>
          </div>
          {f"<div class='actions' style='margin-top:1rem;'>{alternate_code_link}</div>" if alternate_code_link else ""}
        </div>
        <div class="panel">
          <h2>Supplemental artifacts</h2>
          <ul class="mono-list">{extra_links}</ul>
        </div>
      </div>
      <div class="prov-grid">
        {render_provenance_surface('pre', 'Pre-grad graph', pre_grad_text, pre_grad_artifact)}
        {render_provenance_surface('post', 'Post-grad graph', post_grad_text, post_grad_artifact)}
        {render_provenance_surface('code', 'Generated code', code_text, code_artifact)}
      </div>
      <script>{PROVENANCE_SCRIPT}</script>
    </main>
    """
    return page("Provenance detail", body)


def render_provenance_artifact_link(artifact: dict[str, Any] | None, label: str | None = None) -> str:
    if artifact is None:
        return "<span class='muted'>n/a</span>"
    text = label or artifact["title"]
    return f"<a href='/artifacts/{quote(artifact['id'])}'>{escape(text)}</a>"


def render_provenance_surface(
    pane: str,
    title: str,
    text: str,
    artifact: dict[str, Any] | None,
) -> str:
    lines = text.splitlines() or [""]
    rows = "".join(
        f"<div class='prov-line' id='{pane}-L{index}' data-pane='{pane}' data-line='{index}'>"
        f"<a class='prov-ln' href='#{pane}-L{index}'>{index}</a>"
        f"<code>{escape(line) if line else '&nbsp;'}</code>"
        "</div>"
        for index, line in enumerate(lines, start=1)
    )
    artifact_link = render_provenance_artifact_link(artifact, "Open artifact")
    return f"""
    <div class="prov-shell">
      <div class="prov-head">
        <h2>{escape(title)}</h2>
        <div class="actions">{artifact_link}</div>
      </div>
      <div class="prov-surface">{rows}</div>
    </div>
    """


def load_run_manifest(paths: TLHubPaths, run_id: str) -> dict[str, Any] | None:
    path = paths.runs_dir / run_id / "run_manifest.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def render_report_artifacts(report_artifacts: list[dict[str, Any]]) -> str:
    if not report_artifacts:
        return ""
    rows = "".join(
        f"""
        <tr>
          <td><a href="/artifacts/{quote(artifact['id'])}">{escape(artifact['title'])}</a></td>
          <td class="mono">{escape(artifact['relative_path'])}</td>
          <td>{escape(artifact['kind'])}</td>
          <td>{escape(format_artifact_summary(artifact['summary']))}</td>
        </tr>
        """
        for artifact in report_artifacts
    )
    return f"""
    <div class="panel">
      <h2>Reports</h2>
      <p class="section-copy">These synthetic reports close the main `tlparse` parity gaps: <code>raw.jsonl</code>, compile-directory summaries, failures/restarts, export diagnostics, multi-rank analyses, and combined trace outputs.</p>
      <table class="data">
        <thead>
          <tr>
            <th>Report</th>
            <th>Path</th>
            <th>Kind</th>
            <th>Summary</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """


def render_compile_table(run_id: str, compiles: list[dict[str, Any]]) -> str:
    if not compiles:
        return "<div class='empty'>No compile-indexed entries were captured.</div>"
    rows = []
    for compile_entry in compiles:
        metrics = compile_entry.get("compilation_metrics") or {}
        rows.append(
            f"""
            <tr>
              <td class="mono"><a href="/runs/{quote(run_id)}/compiles/{quote(compile_entry['compile_dir'])}">{escape(compile_entry['compile_id'])}</a></td>
              <td>{'' if compile_entry.get('rank') is None else compile_entry['rank']}</td>
              <td>{render_compile_status(compile_entry.get('status', 'missing'))}</td>
              <td>{compile_entry.get('artifact_count', 0)}</td>
              <td>{escape(str(compile_entry.get('cache_status') or 'unknown'))}</td>
              <td>{escape(format_optional_seconds(metrics.get('entire_frame_compile_time_s')))}</td>
              <td>{metrics.get('graph_op_count', 'n/a')}</td>
              <td>{escape(", ".join(compile_entry.get('event_types', [])))}</td>
            </tr>
            """
        )
    return f"""
    <div class="panel">
      <h2>Compile directory</h2>
      <p class="section-copy">Each compile id gets its own detail page with output files, stack, compile-time metrics, custom ops, symbolic shape information, and guard data.</p>
      <table class="data">
        <thead>
          <tr>
            <th>Compile id</th>
            <th>Rank</th>
            <th>Status</th>
            <th>Artifacts</th>
            <th>Cache</th>
            <th>Compile time</th>
            <th>Graph ops</th>
            <th>Event types</th>
          </tr>
        </thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


def render_failures_panel(failures: list[dict[str, Any]]) -> str:
    if not failures:
        return ""
    rows = "".join(
        f"""
        <tr>
          <td class="mono">{escape(str(item.get('compile_id') or 'n/a'))}</td>
          <td>{escape(str(item.get('failure_type') or 'n/a'))}</td>
          <td>{escape(str(item.get('reason') or ''))}</td>
          <td class="mono">{escape(str(item.get('source') or 'n/a'))}</td>
        </tr>
        """
        for item in failures
    )
    return f"""
    <div class="panel">
      <h2>Failures and restarts</h2>
      <table class="data">
        <thead>
          <tr>
            <th>Compile id</th>
            <th>Failure type</th>
            <th>Reason</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """


def render_warnings_panel(warnings: list[str]) -> str:
    if not warnings:
        return ""
    rows = "".join(f"<li>{escape(warning)}</li>" for warning in warnings[:50])
    more = "" if len(warnings) <= 50 else f"<p class='mini-note'>{len(warnings) - 50} additional warning(s) omitted.</p>"
    return f"""
    <div class="panel">
      <h2>Ingest warnings</h2>
      <ul>{rows}</ul>
      {more}
    </div>
    """


def render_export_panel(run_id: str, export: dict[str, Any] | None) -> str:
    if not export:
        return ""
    failures = export.get("failures", [])
    exported_program_artifact_id = export.get("exported_program_artifact_id")
    if not failures and not exported_program_artifact_id:
        return ""
    rows = []
    for failure in failures:
        detail_id = failure.get("detail_id")
        detail_link = (
            f"<a href='/runs/{quote(run_id)}/guards/{quote(detail_id)}'>details</a>"
            if detail_id
            else ""
        )
        rows.append(
            f"""
            <tr>
              <td>{escape(str(failure.get('failure_type') or 'n/a'))}</td>
              <td>{escape(str(failure.get('reason') or ''))}</td>
              <td>{detail_link}</td>
            </tr>
            """
        )
    export_link = (
        f"<a href='/artifacts/{quote(exported_program_artifact_id)}'>View exported program</a>"
        if exported_program_artifact_id
        else "<span class='muted'>No exported program artifact</span>"
    )
    return f"""
    <div class="panel">
      <h2>Export diagnostics</h2>
      <p class="section-copy">{'No export issues were found.' if export.get('success') else f"{len(failures)} export issue(s) were captured."}</p>
      <div class="actions">{export_link}</div>
      {'' if not rows else f"<table class='data'><thead><tr><th>Failure type</th><th>Reason</th><th>Details</th></tr></thead><tbody>{''.join(rows)}</tbody></table>"}
    </div>
    """


def render_vllm_panel(vllm: dict[str, Any] | None) -> str:
    if not vllm:
        return ""
    config = vllm.get("config") or {}
    subgraphs = vllm.get("subgraphs") or []
    piecewise = vllm.get("piecewise_graph")
    config_block = render_json_table(config)
    piecewise_link = (
        f"<a href='/artifacts/{quote(piecewise['id'])}'>View piecewise split graph</a>"
        if piecewise and piecewise.get("id")
        else "<span class='muted'>No piecewise split graph artifact</span>"
    )
    subgraph_blocks = []
    for subgraph in subgraphs:
        artifacts = subgraph.get("artifacts") or []
        artifact_links = (
            "".join(
                f"<li><a href='/artifacts/{quote(artifact['id'])}'>{escape(artifact['title'])}</a></li>"
                for artifact in artifacts
                if artifact.get("id")
            )
            if artifacts
            else "<li class='muted'>No subgraph artifacts were captured.</li>"
        )
        subgraph_blocks.append(
            f"""
            <details class="panel" open>
              <summary>{escape(str(subgraph.get('submod_name') or f"subgraph_{subgraph.get('index')}"))}</summary>
              <div class="summary-list" style="margin-top:0.8rem;">
                <div><span class="muted">Compile range</span><span class="mono">{escape(str(subgraph.get('compile_range_start')))} - {escape(str(subgraph.get('compile_range_end')))}</span></div>
                <div><span class="muted">Single size</span><span class="mono">{escape(str(bool(subgraph.get('is_single_size'))))}</span></div>
                <div><span class="muted">CUDAGraph size</span><span class="mono">{escape(str(bool(subgraph.get('is_cudagraph_size'))))}</span></div>
              </div>
              <ul class="mono-list" style="margin-top:0.8rem;">{artifact_links}</ul>
            </details>
            """
        )
    return f"""
    <div class="panel">
      <h2>vLLM summary</h2>
      <p class="section-copy">The vLLM-specific view keeps the piecewise split graph, compile configuration, and per-subgraph artifact lists together so you can jump straight into the generated graph dumps.</p>
      <div class="grid-2">
        <div class="panel">
          <h3>Compilation config</h3>
          {config_block}
        </div>
        <div class="panel">
          <h3>Piecewise graph</h3>
          <div class="actions">{piecewise_link}</div>
        </div>
      </div>
      {'' if not subgraph_blocks else f"<div class='stack' style='margin-top:1rem;'>{''.join(subgraph_blocks)}</div>"}
    </div>
    """


def render_multi_rank_panel(multi_rank: dict[str, Any] | None) -> str:
    if not multi_rank:
        return ""
    divergence = multi_rank.get("divergence") or {}
    runtime = multi_rank.get("runtime_analysis") or {}
    exec_order = multi_rank.get("exec_order") or {}
    sections = [
        render_grouping_block("Compile id groups", multi_rank.get("compile_id_groups", [])),
        render_grouping_block("Cache pattern groups", multi_rank.get("cache_groups", [])),
        render_grouping_block("Collective groups", multi_rank.get("collective_groups", [])),
        render_grouping_block("Tensor meta groups", multi_rank.get("tensor_meta_groups", [])),
    ]
    runtime_rows = ""
    if runtime:
        runtime_rows = (
            "<div class='muted'>Runtime analysis unavailable.</div>"
            if runtime.get("has_mismatched_graph_counts")
            else "".join(
                f"<div><span class='muted'>{escape(str(graph.get('graph_id')))}</span><span class='mono'>{escape(str(graph.get('delta_ms')))} ms delta</span></div>"
                for graph in runtime.get("graphs", [])
            )
        )
        if runtime_rows:
            runtime_rows = f"<div class='summary-list'>{runtime_rows}</div>"
    exec_order_rows = (
        "<div class='muted'>Execution-order analysis unavailable.</div>"
        if not exec_order
        else render_metrics_list(
            {
                "Execution order differs": exec_order.get("order_differs"),
                "Schedule mismatch": exec_order.get("has_schedule_mismatch"),
                "Cache mismatch": exec_order.get("has_cache_mismatch"),
                "Schedule mismatch ranks": exec_order.get("ranks_schedule_str") or "none",
                "Cache mismatch ranks": exec_order.get("ranks_cache_str") or "none",
            }
        )
    )
    return f"""
    <div class="panel">
      <h2>Multi-rank diagnostics</h2>
      <div class="kpis">
        <div class="kpi"><span class="label">Ranks</span><span class="value">{multi_rank.get('num_ranks', 0)}</span></div>
        <div class="kpi"><span class="label">Compile divergence</span><span class="value">{'yes' if divergence.get('compile_ids') else 'no'}</span></div>
        <div class="kpi"><span class="label">Cache divergence</span><span class="value">{'yes' if divergence.get('cache') else 'no'}</span></div>
        <div class="kpi"><span class="label">Collective divergence</span><span class="value">{'yes' if divergence.get('collective') else 'no'}</span></div>
        <div class="kpi"><span class="label">Tensor meta divergence</span><span class="value">{'yes' if divergence.get('tensor_meta') else 'no'}</span></div>
        <div class="kpi"><span class="label">Chromium events</span><span class="value">{'yes' if multi_rank.get('has_chromium_events') else 'no'}</span></div>
      </div>
      <div class="grid-2" style="margin-top:1rem;">
        <div class="panel">
          <h3>Runtime analysis</h3>
          {runtime_rows or "<div class='muted'>No runtime estimates were captured.</div>"}
        </div>
        <div class="panel">
          <h3>Execution-order analysis</h3>
          {exec_order_rows}
        </div>
      </div>
      <div class="grid-2" style="margin-top:1rem;">
        {''.join(section for section in sections if section)}
      </div>
    </div>
    """


def render_grouping_block(title: str, groups: list[dict[str, Any]]) -> str:
    if not groups:
        return ""
    rows = "".join(
        f"<div><span class='muted'>{escape(str(group.get('ranks') or ''))}</span><span class='mono'>{escape(trim(str(group.get('signature') or ''), 90))}</span></div>"
        for group in groups
    )
    return f"""
    <div class="panel">
      <h3>{escape(title)}</h3>
      <div class="summary-list">{rows}</div>
    </div>
    """


def render_stack_trie(run_id: str, compiles: list[dict[str, Any]]) -> str:
    tree = build_stack_tree(compiles)
    if not tree["children"]:
        return ""
    return f"""
    <div class="panel">
      <h2>Stack trie</h2>
      <p class="section-copy">This mirrors the core `tlparse` orientation view: compile stacks are grouped into a tree so it is obvious where PT2 compilation was triggered and which compile ids share prefixes.</p>
      <div class="stack-tree">{render_stack_tree_children(run_id, tree['children'])}</div>
    </div>
    """


def render_unknown_stacks(stacks: list[list[dict[str, Any]]]) -> str:
    if not stacks:
        return ""
    blocks = "".join(
        f"<div class='frame-block'>{render_stack_frames(stack)}</div>"
        for stack in stacks[:5]
    )
    extra = "" if len(stacks) <= 5 else f"<p class='mini-note'>{len(stacks) - 5} additional unknown stack(s) omitted.</p>"
    return f"""
    <div class="panel">
      <h2>Unknown stacks</h2>
      <p class="section-copy">These log entries carried stack data without compile context, which is the same gap `tlparse` flags in its unknown-stack trie.</p>
      {blocks}
      {extra}
    </div>
    """


def build_stack_tree(compiles: list[dict[str, Any]]) -> dict[str, Any]:
    root = {"children": {}}
    for compile_entry in compiles:
        stack = compile_entry.get("stack") or []
        if not stack:
            continue
        node = root
        for frame in stack:
            key = frame_identity(frame)
            children = node.setdefault("children", {})
            child = children.setdefault(key, {"frame": frame, "children": {}, "compiles": []})
            node = child
        node.setdefault("compiles", []).append(
            {
                "compile_id": compile_entry["compile_id"],
                "compile_dir": compile_entry["compile_dir"],
                "status": compile_entry.get("status", "missing"),
            }
        )
    return root


def render_stack_tree_children(run_id: str, children: dict[str, Any]) -> str:
    items = []
    for child in children.values():
        frame_html = render_frame_label(child["frame"])
        compile_links = "".join(
            f" <a class='pill {status_class(item.get('status'))}' href='/runs/{quote(run_id)}/compiles/{quote(item['compile_dir'])}'>{escape(item['compile_id'])}</a>"
            for item in child.get("compiles", [])
        )
        nested = render_stack_tree_children(run_id, child.get("children", {}))
        items.append(f"<li><span class='stack-frame'>{frame_html}</span>{compile_links}{nested}</li>")
    return "<ul>" + "".join(items) + "</ul>"


def render_stack_frames(frames: list[dict[str, Any]]) -> str:
    if not frames:
        return "<div class='muted'>No stack captured.</div>"
    return "".join(
        f"""
        <div class="frame-block">
          <div class="frame-line">{render_frame_label(frame)}</div>
          {f"<div class='frame-loc'>{escape(frame.get('loc') or '')}</div>" if frame.get('loc') else ""}
        </div>
        """
        for frame in frames
    )


def render_frame_label(frame: dict[str, Any]) -> str:
    return (
        f"{escape(str(frame.get('filename') or '(unknown)'))}:"
        f"{escape(str(frame.get('line') or 0))} in "
        f"{escape(str(frame.get('name') or ''))}"
    )


def render_compile_status(status: str) -> str:
    return render_pill(status, status_class(status))


def status_class(status: str) -> str:
    return {
        "ok": "status-ok",
        "break": "status-break",
        "empty": "status-empty",
        "error": "status-error",
        "missing": "status-missing",
    }.get(status, "status-missing")


def render_compile_artifacts(artifacts: list[dict[str, Any]]) -> str:
    if not artifacts:
        return "<div class='muted'>No output files recorded.</div>"
    rows = "".join(
        f"<li><a href='/artifacts/{quote(artifact['id'])}'>{escape(artifact['relative_path'].split('/')[-1])}</a> <span class='mini-note'>({artifact['number']})</span></li>"
        for artifact in artifacts
    )
    return f"<ul class='mono-list'>{rows}</ul>"


def render_compile_failures(metrics: dict[str, Any]) -> str:
    rows = []
    if metrics.get("fail_type"):
        rows.append(
            f"<div><span class='muted'>Failure</span><span class='mono'>{escape(str(metrics.get('fail_type')))} | {escape(str(metrics.get('fail_reason') or ''))}</span></div>"
        )
    restart_reasons = metrics.get("restart_reasons") or []
    if restart_reasons:
        rows.append(
            f"<div><span class='muted'>Restarts</span><span class='mono'>{escape(', '.join(str(item) for item in restart_reasons))}</span></div>"
        )
    if not rows:
        return "<div class='muted'>No failures or restarts recorded.</div>"
    return f"<div class='summary-list'>{''.join(rows)}</div>"


def render_compile_guard_links(run_id: str, compile_entry: dict[str, Any], manifest: dict[str, Any]) -> str:
    guard_ids = compile_entry.get("export_guards") or []
    if not guard_ids:
        return ""
    rows = "".join(
        f"<li><a href='/runs/{quote(run_id)}/guards/{quote(guard_id)}'>{escape(guard_id)}</a></li>"
        for guard_id in guard_ids
    )
    return f"<div style='margin-top:1rem;'><div class='mini-note'>Export guard details</div><ul class='mono-list'>{rows}</ul></div>"


def render_compile_provenance_links(
    run_id: str,
    compile_entry: dict[str, Any],
    provenance_groups: list[dict[str, Any]],
) -> str:
    matches = [
        group
        for group in provenance_groups
        if (
            compile_entry.get("compile_dir")
            and group.get("compile_dir") == compile_entry.get("compile_dir")
        )
        or (
            compile_entry.get("compile_id")
            and group.get("compile_id") == compile_entry.get("compile_id")
        )
    ]
    if not matches:
        return ""
    rows = "".join(
        f"<li><a href='/runs/{quote(run_id)}/provenance/{quote(group['id'])}'>{escape(group['label'])}</a></li>"
        for group in matches
    )
    return f"<div style='margin-top:1rem;'><div class='mini-note'>Provenance tracking</div><ul class='mono-list'>{rows}</ul></div>"


def render_specialization_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<div class='muted'>No symbolic shape specializations recorded.</div>"
    body = "".join(
        f"""
        <tr>
          <td>{escape(str(row.get('symbol') or ''))}</td>
          <td>{escape(', '.join(str(item) for item in row.get('sources') or []))}</td>
          <td>{escape(str(row.get('value') or ''))}</td>
          <td>{render_small_stack(row.get('user_stack', []))}</td>
          <td>{render_small_stack(row.get('framework_stack', []))}</td>
        </tr>
        """
        for row in rows
    )
    return f"<table class='data'><thead><tr><th>Sym</th><th>Sources</th><th>Value</th><th>User stack</th><th>Framework stack</th></tr></thead><tbody>{body}</tbody></table>"


def render_guard_fast_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<div class='muted'>No guard-added-fast records.</div>"
    body = "".join(
        f"""
        <tr>
          <td>{escape(str(row.get('expr') or ''))}</td>
          <td>{render_small_stack(row.get('user_stack', []))}</td>
          <td>{render_small_stack(row.get('framework_stack', []))}</td>
        </tr>
        """
        for row in rows
    )
    return f"<table class='data'><thead><tr><th>Expr</th><th>User stack</th><th>Framework stack</th></tr></thead><tbody>{body}</tbody></table>"


def render_symbol_table(rows: list[dict[str, Any]], *, created: bool) -> str:
    if not rows:
        return "<div class='muted'>No symbols recorded.</div>"
    if created:
        body = "".join(
            f"""
            <tr>
              <td>{escape(str(row.get('symbol') or ''))}</td>
              <td>{escape(str(row.get('val') or ''))}</td>
              <td>{escape(str(row.get('vr') or ''))}</td>
              <td>{escape(str(row.get('source') or ''))}</td>
              <td>{render_small_stack(row.get('user_stack', []))}</td>
              <td>{render_small_stack(row.get('framework_stack', []))}</td>
            </tr>
            """
            for row in rows
        )
        return f"<table class='data'><thead><tr><th>Symbol</th><th>Value</th><th>Range</th><th>Source</th><th>User stack</th><th>Framework stack</th></tr></thead><tbody>{body}</tbody></table>"
    body = "".join(
        f"""
        <tr>
          <td>{escape(str(row.get('symbol') or ''))}</td>
          <td>{escape(str(row.get('vr') or ''))}</td>
          <td>{render_small_stack(row.get('user_stack', []))}</td>
          <td>{render_small_stack(row.get('framework_stack', []))}</td>
        </tr>
        """
        for row in rows
    )
    return f"<table class='data'><thead><tr><th>Symbol</th><th>Range</th><th>User stack</th><th>Framework stack</th></tr></thead><tbody>{body}</tbody></table>"


def render_compile_links(links: list[dict[str, Any]]) -> str:
    if not links:
        return "<div class='muted'>No external links recorded.</div>"
    items = "".join(
        f"<li><a href='{escape(str(link.get('url') or ''))}'>{escape(str(link.get('name') or link.get('url') or 'link'))}</a></li>"
        for link in links
    )
    return f"<ul class='mono-list'>{items}</ul>"


def render_json_table(data: dict[str, Any]) -> str:
    if not data:
        return "<div class='muted'>No additional data.</div>"
    rows = "".join(
        f"<div><span class='muted'>{escape(str(key))}</span><span class='mono'>{escape(format_summary_value(value))}</span></div>"
        for key, value in data.items()
    )
    return f"<div class='summary-list'>{rows}</div>"


def render_metrics_list(data: dict[str, Any]) -> str:
    filtered = {key: value for key, value in data.items() if value is not None and value != ""}
    if not filtered:
        return "<div class='muted'>No metrics recorded.</div>"
    rows = "".join(
        f"<div><span class='muted'>{escape(str(key))}</span><span class='mono'>{escape(format_summary_value(value))}</span></div>"
        for key, value in filtered.items()
    )
    return f"<div class='summary-list'>{rows}</div>"


def render_small_stack(frames: list[dict[str, Any]]) -> str:
    if not frames:
        return "<span class='muted'>n/a</span>"
    return "<br>".join(render_frame_label(frame) for frame in frames[:3])


def render_expression_tree(tree: dict[str, Any] | None) -> str:
    if not tree:
        return "<div class='muted'>No symbolic expression graph recorded.</div>"
    children = "".join(render_expression_tree(child) for child in tree.get("children", []))
    body = f"""
    <details open>
      <summary><span class='mono'>{escape(str(tree.get('result') or 'expr'))}</span> <span class='mini-note'>{escape(str(tree.get('method') or ''))}</span></summary>
      <div class='summary-list'>
        <div><span class='muted'>Arguments</span><span class='mono'>{escape(', '.join(str(arg) for arg in tree.get('arguments') or []))}</span></div>
      </div>
      <div style='margin-top:0.75rem;'>{children}</div>
    </details>
    """
    return body


def format_rank_count(multi_rank: dict[str, Any] | None) -> str:
    if not multi_rank:
        return "1"
    return str(multi_rank.get("num_ranks", 1))


def frame_identity(frame: dict[str, Any]) -> str:
    return f"{frame.get('filename')}:{frame.get('line')}:{frame.get('name')}:{frame.get('loc')}"


def format_optional_seconds(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.3f} s"
    except (TypeError, ValueError):
        return str(value)


def render_compare_picker(
    runs: list[dict[str, Any]],
    *,
    left_run: str | None = None,
    right_run: str | None = None,
    family: str = "",
) -> str:
    left_options = "".join(
        f"<option value='{escape(run['id'])}'{' selected' if run['id'] == left_run else ''}>"
        f"{escape(run['id'])} | {escape(trim(run['command_display'], 70))}</option>"
        for run in runs
    )
    right_options = "".join(
        f"<option value='{escape(run['id'])}'{' selected' if run['id'] == right_run else ''}>"
        f"{escape(run['id'])} | {escape(trim(run['command_display'], 70))}</option>"
        for run in runs
    )
    return f"""
    <div class="panel">
      <h2>Compare</h2>
      <form action="/compare" method="get" class="toolbar">
        <label>Left run <select name="left_run">{left_options}</select></label>
        <label>Right run <select name="right_run">{right_options}</select></label>
        <label>Family filter <input type="text" name="family" value="{escape(family)}" placeholder="optional substring"></label>
        <button type="submit">Open compare</button>
      </form>
    </div>
    """


def render_arbitrary_diff_picker(
    left_artifacts: list[dict[str, Any]],
    right_artifacts: list[dict[str, Any]],
) -> str:
    if not left_artifacts or not right_artifacts:
        return "<div class='panel'><h2>Pick artifacts</h2><div class='empty'>Both runs need indexed artifacts before a manual diff can be selected.</div></div>"

    left_options = "".join(render_artifact_option(artifact) for artifact in left_artifacts)
    right_options = "".join(render_artifact_option(artifact) for artifact in right_artifacts)
    return f"""
    <div class="panel">
      <h2>Pick any two artifacts</h2>
      <form action="/diff" method="get" class="stack">
        <label>Left artifact <select name="left">{left_options}</select></label>
        <label>Right artifact <select name="right">{right_options}</select></label>
        <button type="submit">Diff artifacts</button>
      </form>
    </div>
    """


def render_match_table(
    left_artifacts: list[dict[str, Any]],
    right_artifacts: list[dict[str, Any]],
) -> str:
    left_map = {artifact["match_key"]: artifact for artifact in left_artifacts}
    right_map = {artifact["match_key"]: artifact for artifact in right_artifacts}
    keys = sorted(set(left_map) | set(right_map))
    if not keys:
        return "<div class='empty'>No artifacts match the current filter.</div>"

    rows = []
    for key in keys:
        left = left_map.get(key)
        right = right_map.get(key)
        if left and right:
            state = "same" if left["sha256"] == right["sha256"] else "changed"
            action = f"<a href='/diff?{urlencode({'left': left['id'], 'right': right['id']})}'>Diff</a>"
        else:
            state = "missing"
            action = ""
        rows.append(
            f"""
            <tr>
              <td class="mono">{escape(key)}</td>
              <td>{render_matched_artifact_cell(left)}</td>
              <td>{render_matched_artifact_cell(right)}</td>
              <td>{render_pill(state, state)}</td>
              <td>{action}</td>
            </tr>
            """
        )

    return f"""
    <div class="panel">
      <h2>Matched families</h2>
      <table class="data">
        <thead>
          <tr>
            <th>Match key</th>
            <th>Left</th>
            <th>Right</th>
            <th>Status</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>
    """


def render_run_row(run: dict[str, Any], manifest: dict[str, Any] | None) -> str:
    compare_url = "/compare?" + urlencode({"left_run": run["id"]})
    delete_action = f"/runs/{quote(run['id'])}/delete"
    compile_count = manifest.get("compile_count", 0) if manifest else 0
    rank_count = format_rank_count(manifest.get("multi_rank") if manifest else None)
    return f"""
    <tr>
      <td class="mono"><a href="/runs/{quote(run['id'])}">{escape(run['id'])}</a></td>
      <td>{render_status(run['status'])}</td>
      <td class="mono">{escape(trim(run['command_display'], 88))}</td>
      <td>{escape(run['started_at'])}</td>
      <td>{escape(format_duration(run['duration_ms']))}</td>
      <td>{run['artifact_count']}</td>
      <td>{compile_count}</td>
      <td>{rank_count}</td>
      <td>{'' if run['exit_code'] is None else run['exit_code']}</td>
      <td>
        <div class="actions">
          <a href="/runs/{quote(run['id'])}">View</a>
          <a href="{compare_url}">Compare</a>
          <form class="inline" action="{delete_action}" method="post">
            <button class="danger" type="submit">Delete</button>
          </form>
        </div>
      </td>
    </tr>
    """


def render_artifact_row(artifact: dict[str, Any]) -> str:
    compare_url = "/compare?" + urlencode({"left_run": artifact["run_id"], "family": artifact["family"]})
    origin = f"{artifact['log_file']}:{artifact['line_no']}"
    if artifact["compile_id"]:
        origin += f" | {artifact['compile_id']}"
    return f"""
    <tr>
      <td><a href="/artifacts/{quote(artifact['id'])}">{escape(artifact['title'])}</a></td>
      <td class="mono">{escape(artifact['match_key'])}</td>
      <td>{escape(artifact['kind'])}</td>
      <td class="mono">{escape(origin)}</td>
      <td>{escape(format_artifact_summary(artifact['summary']))}</td>
      <td>
        <div class="actions">
          <a href="/artifacts/{quote(artifact['id'])}">View</a>
          <a href="{compare_url}">Compare family</a>
        </div>
      </td>
    </tr>
    """


def render_artifact_option(artifact: dict[str, Any]) -> str:
    label = f"{artifact['title']} | {artifact['match_key']}"
    return f"<option value='{escape(artifact['id'])}'>{escape(trim(label, 120))}</option>"


def render_matched_artifact_cell(artifact: dict[str, Any] | None) -> str:
    if artifact is None:
        return "<span class='muted'>missing</span>"
    return (
        f"<a href='/artifacts/{quote(artifact['id'])}'>{escape(artifact['title'])}</a>"
        f"<div class='muted mono'>{escape(artifact['relative_path'])}</div>"
    )


def render_status(status: str) -> str:
    klass = "running" if status == "running" else "failed" if status == "failed" else "finished"
    return render_pill(status, klass)


def render_pill(label: str, klass: str) -> str:
    return f"<span class='pill {escape(klass)}'>{escape(label)}</span>"


def render_summary_card(summary: dict[str, Any]) -> str:
    if not summary:
        return "<div class='muted'>No summary available.</div>"
    rows = []
    for key, value in summary.items():
        rows.append(
            f"<div><span class='muted'>{escape(key)}</span><span class='mono'>{escape(format_summary_value(value))}</span></div>"
        )
    return f"<div class='summary-list'>{''.join(rows)}</div>"


def render_diff_side(label: str, artifact: dict[str, Any]) -> str:
    return f"""
    <div class="panel">
      <h2>{escape(label)}</h2>
      <div class="summary-list">
        <div><span class="muted">Artifact</span><span>{escape(artifact['title'])}</span></div>
        <div><span class="muted">Run</span><span class="mono"><a href="/runs/{quote(artifact['run_id'])}">{escape(artifact['run_id'])}</a></span></div>
        <div><span class="muted">Match key</span><span class="mono">{escape(artifact['match_key'])}</span></div>
        <div><span class="muted">Path</span><span class="mono">{escape(artifact['relative_path'])}</span></div>
        <div><span class="muted">SHA</span><span class="mono">{escape(artifact['sha256'][:16])}</span></div>
      </div>
    </div>
    """


def render_diff_summary(left: dict[str, Any], right: dict[str, Any]) -> str:
    keys = sorted(set(left) | set(right))
    if not keys:
        return "<div class='muted'>No summary data available.</div>"
    rows = []
    for key in keys:
        rows.append(
            f"<div><span class='muted'>{escape(key)}</span><span class='mono'>{escape(format_summary_value(left.get(key)))} -> {escape(format_summary_value(right.get(key)))}</span></div>"
        )
    return f"<div class='summary-list'>{''.join(rows)}</div>"


def render_semantic_diff(
    left_artifact: dict[str, Any],
    right_artifact: dict[str, Any],
    left_text: str,
    right_text: str,
) -> str:
    if left_artifact.get("kind") == right_artifact.get("kind") == "fx_graph":
        graph_diff = build_fx_graph_diff(left_text, right_text)
        if graph_diff is None:
            return ""
        added = "".join(
            f"<li><span class='mono'>{escape(item['name'])}</span> <span class='muted'>{escape(item['target'])}</span></li>"
            for item in graph_diff["added"][:24]
        ) or "<li class='muted'>No added nodes.</li>"
        removed = "".join(
            f"<li><span class='mono'>{escape(item['name'])}</span> <span class='muted'>{escape(item['target'])}</span></li>"
            for item in graph_diff["removed"][:24]
        ) or "<li class='muted'>No removed nodes.</li>"
        changed = "".join(
            f"<tr><td class='mono'>{escape(item['name'])}</td><td class='mono'>{escape(item['left']['target'])}</td><td class='mono'>{escape(item['right']['target'])}</td></tr>"
            for item in graph_diff["changed"][:40]
        )
        changed_block = (
            "<div class='muted'>No node-level target changes.</div>"
            if not changed
            else f"<table class='data'><thead><tr><th>Node</th><th>Left target</th><th>Right target</th></tr></thead><tbody>{changed}</tbody></table>"
        )
        return f"""
        <div class="panel">
          <h2>Graph structure diff</h2>
          <p class="section-copy">This diff is semantic rather than text-only: FX nodes are parsed and compared by name and emitted target so you can spot graph-shape changes faster than reading raw line diffs.</p>
          <div class="kpis">
            <div class="kpi"><span class="label">Left nodes</span><span class="value">{graph_diff['left_count']}</span></div>
            <div class="kpi"><span class="label">Right nodes</span><span class="value">{graph_diff['right_count']}</span></div>
            <div class="kpi"><span class="label">Added</span><span class="value">{len(graph_diff['added'])}</span></div>
            <div class="kpi"><span class="label">Removed</span><span class="value">{len(graph_diff['removed'])}</span></div>
            <div class="kpi"><span class="label">Changed</span><span class="value">{len(graph_diff['changed'])}</span></div>
            <div class="kpi"><span class="label">Order changed</span><span class="value">{'yes' if graph_diff['order_changed'] else 'no'}</span></div>
          </div>
          <div class="grid-2" style="margin-top:1rem;">
            <div class="panel">
              <h3>Added nodes</h3>
              <ul class="mono-list">{added}</ul>
            </div>
            <div class="panel">
              <h3>Removed nodes</h3>
              <ul class="mono-list">{removed}</ul>
            </div>
          </div>
          <div class="panel" style="margin-top:1rem;">
            <h3>Changed nodes</h3>
            {changed_block}
          </div>
        </div>
        """

    if left_artifact.get("kind") == right_artifact.get("kind") == "json":
        json_diff = build_json_diff(left_text, right_text)
        if json_diff is None:
            return ""
        return f"""
        <div class="panel">
          <h2>Structured JSON diff</h2>
          <div class="grid-2">
            <div class="panel">
              <h3>Added keys</h3>
              <ul class="mono-list">{''.join(f"<li>{escape(item)}</li>" for item in json_diff['added']) or "<li class='muted'>No added keys.</li>"}</ul>
            </div>
            <div class="panel">
              <h3>Removed keys</h3>
              <ul class="mono-list">{''.join(f"<li>{escape(item)}</li>" for item in json_diff['removed']) or "<li class='muted'>No removed keys.</li>"}</ul>
            </div>
          </div>
          <div class="panel" style="margin-top:1rem;">
            <h3>Changed keys</h3>
            <ul class="mono-list">{''.join(f"<li>{escape(item)}</li>" for item in json_diff['changed']) or "<li class='muted'>No changed keys.</li>"}</ul>
          </div>
        </div>
        """

    return ""


def read_artifact_text(paths: TLHubPaths, artifact: dict[str, Any]) -> str:
    path = paths.runs_dir / artifact["run_id"] / "artifacts" / artifact["relative_path"]
    return path.read_text(encoding="utf-8", errors="replace")


def render_source_block(text: str) -> str:
    lines = text.splitlines() or [""]
    rows = "".join(
        f"<tr id='L{index}'><td class='ln'><a href='#L{index}'>{index}</a></td><td class='code'><code>{escape(line) if line else '&nbsp;'}</code></td></tr>"
        for index, line in enumerate(lines, start=1)
    )
    return f"<table class='source'><tbody>{rows}</tbody></table>"


def format_artifact_summary(summary: dict[str, Any]) -> str:
    parts: list[str] = []
    if "node_count" in summary:
        parts.append(f"nodes {summary['node_count']}")
    if "top_level_keys" in summary:
        parts.append(f"keys {summary['top_level_keys']}")
    if "top_level_items" in summary:
        parts.append(f"items {summary['top_level_items']}")
    if "preview" in summary:
        parts.append(trim(str(summary["preview"]), 80))
    return " | ".join(parts) or "n/a"


def format_summary_value(value: Any) -> str:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if value is None:
        return "n/a"
    return str(value)


def format_duration(duration_ms: int | None) -> str:
    if duration_ms is None:
        return "n/a"
    if duration_ms < 1000:
        return f"{duration_ms} ms"
    seconds = duration_ms / 1000
    if seconds < 60:
        return f"{seconds:.2f} s"
    minutes = seconds / 60
    return f"{minutes:.2f} min"


def trim(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def first_param(params: dict[str, list[str]], name: str) -> str | None:
    values = params.get(name)
    if not values:
        return None
    return values[0]


def page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(title)}</title>
    <style>{APP_CSS}</style>
  </head>
  <body>{body}</body>
</html>"""


def escape(value: str) -> str:
    return html.escape(value, quote=True)

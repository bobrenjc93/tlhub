from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import time
from typing import Sequence
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import urlopen
import uuid
import webbrowser

from tlhub import __version__
from tlhub.config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    build_user_url,
    ensure_layout,
    get_paths,
)
from tlhub.database import Repository, RunCreate
from tlhub.server import run_daemon
from tlhub.trace_parser import ParseResult, ingest_trace_dir


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv if argv is not None else sys.argv[1:]))

    paths = ensure_layout(get_paths())

    if args.serve_daemon:
        run_daemon(paths, host=args.host, port=args.port)
        return 0

    if args.stop:
        command = normalize_command(args.command)
        if command:
            fail("--stop does not accept a wrapped command")
        stopped = stop_daemon(paths)
        print("stopped" if stopped else "not running")
        return 0 if stopped else 1

    command = normalize_command(args.command)
    if not command:
        url = ensure_daemon_running(paths, preferred_port=args.port)
        print(url)
        webbrowser.open(url, new=2)
        return 0

    return run_wrapped_command(command, preferred_port=args.port)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tlhub",
        description=(
            "Wrap a command with TORCH_TRACE capture and keep a local browser-based trace hub. "
            "Use `tlhub --stop` to stop the background daemon. "
            "Set BASE_URL to rewrite printed and opened URLs when running behind a proxy."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--stop", action="store_true", help="stop the background tlhub daemon")
    parser.add_argument("--serve-daemon", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--host", default=DEFAULT_HOST, help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER, help="command to execute")
    return parser


def run_wrapped_command(command: list[str], *, preferred_port: int) -> int:
    if not command:
        fail("no command provided")

    paths = ensure_layout(get_paths())
    repo = Repository(paths)
    base_url = ensure_daemon_running(paths, preferred_port=preferred_port)

    run_id = new_run_id()
    run_dir = paths.runs_dir / run_id
    trace_dir = run_dir / "trace"
    artifacts_dir = run_dir / "artifacts"
    trace_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    created_at = iso_now()
    repo.create_run(
        RunCreate(
            id=run_id,
            status="running",
            command=command,
            command_display=shlex.join(command),
            cwd=os.getcwd(),
            hostname=socket.gethostname(),
            trace_dir=str(trace_dir),
            created_at=created_at,
            started_at=created_at,
        )
    )

    env = os.environ.copy()
    env["TORCH_TRACE"] = str(trace_dir)
    env["TLHUB_RUN_ID"] = run_id

    print(f"tlhub: run {run_id}")
    print(f"tlhub: TORCH_TRACE={trace_dir}")

    exit_code = 127
    command_started = time.perf_counter()
    try:
        completed = subprocess.run(command, env=env)
    except FileNotFoundError:
        print(f"tlhub: command not found: {command[0]}", file=sys.stderr)
        exit_code = 127
    except KeyboardInterrupt:
        print("tlhub: interrupted", file=sys.stderr)
        exit_code = 130
    else:
        exit_code = completed.returncode

    duration_ms = int((time.perf_counter() - command_started) * 1000)
    finished_at = iso_now()

    parse_result = safe_ingest(run_id, trace_dir, artifacts_dir)
    repo.finish_run(
        run_id,
        status="finished" if exit_code == 0 else "failed",
        finished_at=finished_at,
        duration_ms=duration_ms,
        exit_code=exit_code,
        log_count=len(parse_result.log_files),
        artifacts=parse_result.artifacts,
    )

    if parse_result.warnings:
        print("tlhub: ingest warnings:", file=sys.stderr)
        for warning in parse_result.warnings[:20]:
            print(f"  {warning}", file=sys.stderr)
        if len(parse_result.warnings) > 20:
            print(f"  ... {len(parse_result.warnings) - 20} more", file=sys.stderr)

    run_url = build_public_url(base_url, f"/runs/{run_id}")
    print(
        "tlhub: indexed "
        f"{len(parse_result.artifacts)} artifacts from {len(parse_result.log_files)} log files"
    )
    print(f"tlhub: {run_url}")
    return exit_code


def normalize_command(command: list[str]) -> list[str]:
    if command and command[0] == "--":
        return command[1:]
    return command


def safe_ingest(run_id: str, trace_dir: Path, artifacts_dir: Path) -> ParseResult:
    try:
        return ingest_trace_dir(run_id, trace_dir, artifacts_dir)
    except Exception as error:  # noqa: BLE001
        return ParseResult(
            artifacts=[],
            log_files=[],
            warnings=[f"ingest failed: {error}"],
            manifest={},
        )


def ensure_daemon_running(paths, *, preferred_port: int) -> str:
    running, url = daemon_status(paths)
    if running and url and daemon_version_matches(paths):
        return build_public_url(url)

    pid = running_daemon_pid(paths)
    if pid is not None:
        if not stop_daemon(paths):
            fail("failed to restart existing tlhub daemon; try `tlhub --stop` and retry")

    url = start_daemon(paths, preferred_port=preferred_port)
    if url is None:
        fail("failed to start tlhub daemon; check the daemon log for details")
    return build_public_url(url)


def start_daemon(paths, *, preferred_port: int) -> str | None:
    url = spawn_daemon(paths, preferred_port)
    if url or preferred_port == 0:
        return url
    return spawn_daemon(paths, 0)


def spawn_daemon(paths, port: int) -> str | None:
    cleanup_stale_daemon_files(paths)
    with paths.daemon_log_path.open("ab") as daemon_log:
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tlhub",
                "--serve-daemon",
                "--host",
                DEFAULT_HOST,
                "--port",
                str(port),
            ],
            stdin=subprocess.DEVNULL,
            stdout=daemon_log,
            stderr=daemon_log,
            start_new_session=True,
        )

    deadline = time.time() + 10
    while time.time() < deadline:
        running, url = daemon_status(paths, cleanup_stale=False)
        if running and url:
            return url
        time.sleep(0.1)
    return None


def daemon_status(paths, *, cleanup_stale: bool = True) -> tuple[bool, str | None]:
    pid = read_int(paths.daemon_pid_path)
    port = read_int(paths.daemon_port_path)
    if pid is None or port is None:
        if cleanup_stale:
            cleanup_stale_daemon_files(paths)
        return False, None

    if not process_exists(pid):
        if cleanup_stale:
            cleanup_stale_daemon_files(paths)
        return False, None

    url = f"http://{DEFAULT_HOST}:{port}"
    if check_health(url):
        return True, url
    return False, None


def stop_daemon(paths) -> bool:
    pid = read_int(paths.daemon_pid_path)
    if pid is None:
        cleanup_stale_daemon_files(paths)
        return False

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        cleanup_stale_daemon_files(paths)
        return False

    deadline = time.time() + 5
    while time.time() < deadline:
        if not process_exists(pid):
            cleanup_stale_daemon_files(paths)
            return True
        time.sleep(0.1)

    cleanup_stale_daemon_files(paths)
    return not process_exists(pid)


def cleanup_stale_daemon_files(paths) -> None:
    pid = read_int(paths.daemon_pid_path)
    if pid is not None and process_exists(pid):
        return
    for path in (
        paths.daemon_pid_path,
        paths.daemon_port_path,
        paths.daemon_version_path,
    ):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def check_health(base_url: str) -> bool:
    try:
        with urlopen(f"{base_url}/healthz", timeout=0.25) as response:
            return response.status == 200
    except (OSError, URLError):
        return False


def read_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def read_text(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    return value or None


def daemon_version_matches(paths) -> bool:
    return read_text(paths.daemon_version_path) == __version__


def running_daemon_pid(paths) -> int | None:
    pid = read_int(paths.daemon_pid_path)
    if pid is None or not process_exists(pid):
        return None
    return pid


def iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def new_run_id() -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def build_public_url(base_url: str, path: str = "") -> str:
    split = urlsplit(base_url)
    if split.port is None:
        fail(f"invalid daemon URL: {base_url}")
    try:
        return build_user_url(split.port, path)
    except ValueError as error:
        fail(str(error))


def fail(message: str) -> None:
    raise SystemExit(message)

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9234


@dataclass(frozen=True)
class TLHubPaths:
    home: Path
    db_path: Path
    runs_dir: Path
    logs_dir: Path
    daemon_pid_path: Path
    daemon_port_path: Path
    daemon_log_path: Path


def get_home() -> Path:
    raw = os.environ.get("TLHUB_HOME")
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / ".local" / "share" / "tlhub").resolve()


def get_paths() -> TLHubPaths:
    home = get_home()
    return TLHubPaths(
        home=home,
        db_path=home / "state" / "tlhub.db",
        runs_dir=home / "runs",
        logs_dir=home / "logs",
        daemon_pid_path=home / "state" / "daemon.pid",
        daemon_port_path=home / "state" / "daemon.port",
        daemon_log_path=home / "logs" / "daemon.log",
    )


def ensure_layout(paths: TLHubPaths | None = None) -> TLHubPaths:
    paths = paths or get_paths()
    paths.home.mkdir(parents=True, exist_ok=True)
    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    paths.runs_dir.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    return paths

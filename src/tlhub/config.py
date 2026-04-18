from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9234
BASE_URL_ENV = "BASE_URL"


@dataclass(frozen=True)
class TLHubPaths:
    home: Path
    db_path: Path
    runs_dir: Path
    logs_dir: Path
    daemon_pid_path: Path
    daemon_port_path: Path
    daemon_version_path: Path
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
        daemon_version_path=home / "state" / "daemon.version",
        daemon_log_path=home / "logs" / "daemon.log",
    )


def ensure_layout(paths: TLHubPaths | None = None) -> TLHubPaths:
    paths = paths or get_paths()
    paths.home.mkdir(parents=True, exist_ok=True)
    paths.db_path.parent.mkdir(parents=True, exist_ok=True)
    paths.runs_dir.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    return paths


def parse_base_url() -> SplitResult | None:
    raw = os.environ.get(BASE_URL_ENV)
    if not raw:
        return None

    split = urlsplit(raw)
    if not split.scheme or not split.netloc:
        raise ValueError(f"{BASE_URL_ENV} must be a full URL like http://example.com.")
    if split.hostname is None:
        raise ValueError(
            f"{BASE_URL_ENV} must include a hostname like http://example.com."
        )
    return split


def join_url_path(base_path: str, path: str) -> str:
    prefix = base_path.rstrip("/")
    if not path:
        return prefix

    normalized = path if path.startswith("/") else f"/{path}"
    return f"{prefix}{normalized}" if prefix else normalized


def build_user_url(port: int, path: str = "") -> str:
    split = parse_base_url()
    if split is None:
        return urlunsplit(("http", f"{DEFAULT_HOST}:{port}", join_url_path("", path), "", ""))

    return urlunsplit(
        (
            split.scheme,
            build_netloc(split, port),
            join_url_path(split.path, path),
            split.query,
            "",
        )
    )


def build_app_url(
    path: str = "/",
    *,
    query: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
) -> str:
    split = parse_base_url()
    base_path = split.path if split is not None else ""
    path_value = join_url_path(base_path, path) or "/"
    query_items = parse_qsl(split.query, keep_blank_values=True) if split is not None else []
    if query:
        additions = query.items() if isinstance(query, Mapping) else query
        query_items.extend((key, value) for key, value in additions if value is not None)
    return urlunsplit(("", "", path_value, urlencode(query_items), ""))


def strip_app_path_prefix(path: str) -> str:
    split = parse_base_url()
    if split is None:
        return path or "/"

    prefix = split.path.rstrip("/")
    if not prefix:
        return path or "/"
    if path == prefix:
        return "/"
    if path.startswith(f"{prefix}/"):
        stripped = path[len(prefix) :]
        return stripped or "/"
    return path or "/"


def build_netloc(split: SplitResult, port: int) -> str:
    hostname = split.hostname
    assert hostname is not None
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname

    userinfo = ""
    if split.username:
        userinfo = split.username
        if split.password:
            userinfo += f":{split.password}"
        userinfo += "@"

    return f"{userinfo}{host}:{port}"

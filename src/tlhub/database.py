from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
import sqlite3
from typing import Any, Iterable

from tlhub.config import TLHubPaths, ensure_layout


@dataclass(frozen=True)
class RunCreate:
    id: str
    status: str
    command: list[str]
    command_display: str
    cwd: str
    hostname: str
    trace_dir: str
    created_at: str
    started_at: str


@dataclass(frozen=True)
class ArtifactCreate:
    id: str
    run_id: str
    match_key: str
    family: str
    family_index: int
    kind: str
    title: str
    event_type: str
    relative_path: str
    compile_id: str | None
    compile_dir: str | None
    rank: int | None
    log_file: str
    line_no: int
    encoding: str
    content_type: str
    size_bytes: int
    sha256: str
    summary: dict[str, Any]


class Repository:
    def __init__(self, paths: TLHubPaths) -> None:
        self.paths = ensure_layout(paths)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.paths.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize(self) -> None:
        with closing(self._connect()) as conn, conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    command_json TEXT NOT NULL,
                    command_display TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    trace_dir TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms INTEGER,
                    exit_code INTEGER,
                    artifact_count INTEGER NOT NULL DEFAULT 0,
                    log_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    match_key TEXT NOT NULL,
                    family TEXT NOT NULL,
                    family_index INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    compile_id TEXT,
                    compile_dir TEXT,
                    rank INTEGER,
                    log_file TEXT NOT NULL,
                    line_no INTEGER NOT NULL,
                    encoding TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    summary_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
                CREATE INDEX IF NOT EXISTS idx_artifacts_match_key ON artifacts(match_key);
                """
            )

    def create_run(self, run: RunCreate) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO runs (
                    id, status, command_json, command_display, cwd, hostname, trace_dir,
                    created_at, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.id,
                    run.status,
                    json.dumps(run.command),
                    run.command_display,
                    run.cwd,
                    run.hostname,
                    run.trace_dir,
                    run.created_at,
                    run.started_at,
                ),
            )

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        finished_at: str,
        duration_ms: int,
        exit_code: int,
        log_count: int,
        artifacts: Iterable[ArtifactCreate],
    ) -> None:
        rows = list(artifacts)
        with closing(self._connect()) as conn, conn:
            conn.execute("DELETE FROM artifacts WHERE run_id = ?", (run_id,))
            conn.executemany(
                """
                INSERT INTO artifacts (
                    id, run_id, match_key, family, family_index, kind, title, event_type,
                    relative_path, compile_id, compile_dir, rank, log_file, line_no, encoding,
                    content_type, size_bytes, sha256, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        artifact.id,
                        artifact.run_id,
                        artifact.match_key,
                        artifact.family,
                        artifact.family_index,
                        artifact.kind,
                        artifact.title,
                        artifact.event_type,
                        artifact.relative_path,
                        artifact.compile_id,
                        artifact.compile_dir,
                        artifact.rank,
                        artifact.log_file,
                        artifact.line_no,
                        artifact.encoding,
                        artifact.content_type,
                        artifact.size_bytes,
                        artifact.sha256,
                        json.dumps(artifact.summary, sort_keys=True),
                    )
                    for artifact in rows
                ],
            )
            conn.execute(
                """
                UPDATE runs
                SET status = ?, finished_at = ?, duration_ms = ?, exit_code = ?,
                    artifact_count = ?, log_count = ?
                WHERE id = ?
                """,
                (
                    status,
                    finished_at,
                    duration_ms,
                    exit_code,
                    len(rows),
                    log_count,
                    run_id,
                ),
            )

    def list_runs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM runs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._hydrate_run(row) for row in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return self._hydrate_run(row) if row else None

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM artifacts
                WHERE run_id = ?
                ORDER BY family, family_index, line_no
                """,
                (run_id,),
            ).fetchall()
        return [self._hydrate_artifact(row) for row in rows]

    def get_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()
        return self._hydrate_artifact(row) if row else None

    def delete_run(self, run_id: str) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))

    @staticmethod
    def _hydrate_run(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["command"] = json.loads(data.pop("command_json"))
        return data

    @staticmethod
    def _hydrate_artifact(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["summary"] = json.loads(data.pop("summary_json"))
        return data

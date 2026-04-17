# tlhub

`tlhub` is a local hub for `TORCH_TRACE` runs.

Instead of manually setting `TORCH_TRACE`, running a program, and then feeding the logs into `tlparse`, `tlhub` folds that loop into one tool:

1. Wraps a command and sets `TORCH_TRACE` for it.
2. Indexes the resulting trace logs into browsable artifacts.
3. Keeps a local daemon-backed UI where runs can be viewed, compared, diffed, and deleted.

`tlhub` does not reuse `tlparse`'s HTML renderer. It parses the structured trace logs directly and renders its own viewer, but it now targets the same major report surfaces as `tlparse`: compile summaries, stack trie, failures and restarts, raw JSONL, export diagnostics, provenance tracking, vLLM views, and multi-rank analysis.

## Install

```bash
python3 -m pip install tlhub
```

From source:

```bash
python3 -m pip install -e .
```

## Quick start

Prefix any command with `tlhub`:

```bash
tlhub python train.py
```

That will:

- create a new run directory under `TLHUB_HOME` or `~/.local/share/tlhub`
- set `TORCH_TRACE` for the wrapped command
- check for the background daemon and start it if needed
- ingest the resulting trace logs
- print a run URL you can open in the browser

Open the dashboard without running a command:

```bash
tlhub
```

Stop the daemon:

```bash
tlhub --stop
```

There are no user-facing subcommands. Run deletion and cross-run comparison happen in the web UI.

## Viewer parity targets

The viewer now exposes the main `tlparse` surfaces in `tlhub` form:

- run dashboard
- per-run compile directory
- compile-detail pages with stack, output files, compile metrics, custom op info, symbolic shape specialization, created symbols, unbacked symbols, and guard-added-fast data
- stack trie over compile stacks
- failures and restarts summary
- `raw.jsonl` shortraw-style output with the string table prepended
- export diagnostics plus guard detail pages
- provenance-tracking pages that align pre-grad graphs, post-grad graphs, and generated code via node mappings
- vLLM-specific summary pages with piecewise split graphs, compile config, and per-subgraph artifact listings
- multi-rank diagnostics:
  - compile id divergence
  - cache-pattern divergence
  - collective divergence
  - tensor-meta divergence
  - runtime delta summaries
  - execution-order summaries
- combined and derived report artifacts such as:
  - `chromium_events.json`
  - `runtime_estimations.json`
  - `chromium_trace_with_runtime.json`
  - `collective_schedules.json`
  - `collectives_parity.json`
  - `execution_order_report.json`
  - `compile_directory.json`

The diffing workflow is built into the UI. You can either:

- compare two runs and let `tlhub` line up artifacts by family plus occurrence index
- pick any two artifacts manually and diff them directly

This is especially useful for FX graphs, Inductor output code, and report JSON. FX-graph diffs also get a graph-aware semantic view so added, removed, and retargeted nodes are visible before you drop to raw line diffs.

## What gets indexed

The ingester understands the same raw `TORCH_TRACE` log shape that `tlparse` reads:

- glog-formatted structured log lines
- JSON envelopes
- tab-indented payload bodies following `has_payload`
- string-table (`"str"`) entries used by stack frames

It extracts and stores artifacts such as:

- graph payloads like `dynamo_output_graph`, `aot_*_graph`, and `inductor_*_graph`
- `graph_dump`
- `inductor_output_code`
- `dynamo_guards`
- generic `artifact` payloads with `string` or `json` encoding
- `memoizer_artifacts`
- `dump_file`
- `exported_program`
- synthetic report outputs derived from the run

Artifacts are grouped by a stable family key plus occurrence index so that matching outputs from two runs can be lined up and diffed.

## Web UI

The local UI provides:

- a run dashboard
- a report section for `raw.jsonl` and derived diagnostics
- compile-detail pages
- export-guard detail pages
- provenance detail pages
- per-run artifact browsing
- artifact-family matching across two runs
- arbitrary artifact-to-artifact diffing
- run deletion

For graph-like text artifacts, the viewer records lightweight summaries such as node counts and op buckets to make diffs easier to scan. Synthetic reports are also first-class artifacts, so you can diff analysis outputs across runs, not just raw payloads.

## Daemon behavior

The daemon is local-only and binds to `127.0.0.1`.

Every normal `tlhub` invocation checks whether the daemon is already up and starts it automatically when needed. `tlhub --stop` is the only normal daemon-management command.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

## Publish

Build and validate the distribution locally:

```bash
uv build
uv run --with twine python -m twine check dist/*
```

Publish directly from your machine:

```bash
uv publish
```

The repo also includes a GitHub Actions workflow at `.github/workflows/publish.yml` that publishes on tags like `v0.1.0`.

Before using the workflow, configure PyPI Trusted Publishing for:

- owner or user: `bobrenjc93`
- repository: `tlhub`
- workflow: `publish.yml`
- environment: `pypi`

Release flow:

1. Update `src/tlhub/__init__.py` with the new version.
2. Commit and push `main`.
3. Create and push a matching tag such as `v0.1.0`.

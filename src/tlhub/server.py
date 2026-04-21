from __future__ import annotations

from collections import defaultdict
import difflib
import html
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import signal
import threading
from typing import Any
from datetime import datetime
from urllib.parse import parse_qs, quote, urlencode, urlparse

from tlhub import __version__
from tlhub.config import (
    DEFAULT_HOST,
    TLHubPaths,
    build_app_url,
    ensure_layout,
    strip_app_path_prefix,
)
from tlhub.database import Repository
from tlhub.view_helpers import (
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
  align-items: flex-start;
  gap: 1rem;
}
.title > div:first-child {
  min-width: 0;
  flex: 1 1 auto;
}
.title .actions {
  flex: 0 0 auto;
  margin-top: 0.35rem;
}
.title h1 {
  overflow-wrap: anywhere;
}
.title p {
  overflow-wrap: anywhere;
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
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  gap: 0.3rem;
  min-height: 4.4rem;
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 0.7rem 0.9rem;
  background: rgba(255, 255, 255, 0.55);
}
.kpi .label {
  display: block;
  font-size: 0.72rem;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
.kpi .value {
  display: block;
  font-size: 0.98rem;
  font-weight: 600;
  line-height: 1.25;
  overflow-wrap: anywhere;
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
  width: 100%;
  border-collapse: collapse;
  background: #fffdf9;
  font-size: 0.88rem;
  line-height: 1.55;
}
table.source td.ln {
  width: 1%;
  min-width: 3.2rem;
  padding: 0 0.75rem 0 0.6rem;
  white-space: nowrap;
  text-align: right;
  color: var(--muted);
  background: rgba(191, 153, 121, 0.08);
  border-right: 1px solid var(--line);
  user-select: none;
  font-variant-numeric: tabular-nums;
}
table.source td.ln a {
  color: inherit;
  text-decoration: none;
  opacity: 0.7;
}
table.source td.ln a:hover {
  opacity: 1;
  text-decoration: underline;
}
table.source td.code {
  width: auto;
  padding: 0 0.9rem;
}
table.source code {
  white-space: pre;
  font-family: var(--mono);
  font-size: 0.88rem;
}
table.source tr:hover td.ln {
  background: rgba(191, 153, 121, 0.16);
  color: var(--text);
}
table.source tr:hover td.ln a {
  opacity: 1;
}
table.source tr:target td {
  background: rgba(182, 84, 31, 0.12);
}
.hl-keyword { color: #b6541f; font-weight: 600; }
.hl-const { color: #8f2f1d; font-weight: 600; }
.hl-builtin { color: #6b3f8d; }
.hl-func { color: #265c43; }
.hl-string { color: #2f6a4f; }
.hl-number { color: #996d00; }
.hl-comment { color: #8a7565; font-style: italic; }
.hl-op { color: #8a7565; }
.hl-decor { color: #6b3f8d; font-style: italic; }
.hl-fxname { color: #b6541f; }
.hl-punct { color: #8a7565; }
.lang-chip {
  display: inline-block;
  margin-left: 0.3rem;
  padding: 0.05rem 0.4rem;
  border-radius: 999px;
  background: rgba(182, 84, 31, 0.13);
  color: var(--accent);
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.artifact-content-panel {
  padding: 0;
  overflow: hidden;
}
.artifact-content-panel .panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.8rem;
  padding: 0.85rem 1.1rem;
  border-bottom: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.55);
}
.artifact-content-panel .panel-head h2 {
  margin: 0;
  font-size: 1rem;
  font-family: var(--mono);
  letter-spacing: -0.01em;
  word-break: break-all;
}
.artifact-content-panel .panel-head-actions {
  display: inline-flex;
  gap: 0.4rem;
}
.source-scroll {
  max-height: min(72vh, 56rem);
  overflow: auto;
}
.source-scroll table.source {
  margin: 0;
}
.copy-btn {
  padding: 0.38rem 0.75rem;
  border-radius: 8px;
  border: 1px solid var(--line);
  background: linear-gradient(180deg, #fff9ef, #f2e4d6);
  color: var(--text);
  font-size: 0.82rem;
  cursor: pointer;
  transition: transform 120ms ease, border-color 120ms ease, box-shadow 120ms ease;
}
.copy-btn:hover {
  border-color: rgba(182, 84, 31, 0.4);
  box-shadow: 0 6px 14px rgba(73, 45, 20, 0.06);
  transform: translateY(-1px);
}
.copy-btn.copied {
  color: var(--ok);
  border-color: rgba(38, 92, 67, 0.45);
}
.visually-hidden {
  position: absolute !important;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}
.artifact-breadcrumb {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.4rem;
  margin: 0.6rem 0 0;
  padding: 0.45rem 0.7rem;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.55);
  font-size: 0.85rem;
  color: var(--muted);
  overflow: hidden;
}
.artifact-breadcrumb .sep {
  opacity: 0.5;
}
.artifact-breadcrumb .mono {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  flex: 1 1 auto;
  min-width: 0;
  color: var(--text);
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
.actions > a {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 0.5rem 0.75rem;
  background: linear-gradient(180deg, #fff9ef, #f2e4d6);
  color: var(--text);
  font-size: 0.92rem;
  line-height: 1.1;
  text-decoration: none;
  transition: transform 120ms ease, border-color 120ms ease, box-shadow 120ms ease;
}
.actions > a:hover {
  text-decoration: none;
  border-color: rgba(182, 84, 31, 0.38);
  box-shadow: 0 8px 18px rgba(73, 45, 20, 0.08);
  transform: translateY(-1px);
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
.summary-list .mono {
  text-align: right;
  overflow-wrap: anywhere;
  min-width: 0;
}
.command-block {
  margin-top: 1rem;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.55);
}
.command-block > summary {
  cursor: pointer;
  padding: 0.55rem 0.85rem;
  color: var(--muted);
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  list-style: none;
}
.command-block > summary::-webkit-details-marker { display: none; }
.command-block > summary::before {
  content: "\u25B8  ";
  display: inline-block;
  transition: transform 120ms ease;
}
.command-block[open] > summary::before {
  content: "\u25BE  ";
}
.command-pre {
  margin: 0;
  padding: 0.7rem 0.85rem 0.9rem;
  border-top: 1px solid var(--line);
  background: #fffdf9;
  font-family: var(--mono);
  font-size: 0.84rem;
  line-height: 1.4;
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 14rem;
  overflow: auto;
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
.stack-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  margin: 0 0 0.9rem;
}
.ir-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: grid;
  gap: 0.75rem;
}
.ir-compile {
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 0.75rem 0.9rem;
  background: rgba(255, 255, 255, 0.55);
}
.ir-compile-head {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  margin-bottom: 0.45rem;
}
.ir-compile-id {
  text-decoration: none;
  font-family: var(--mono);
}
.ir-compile-id:hover {
  text-decoration: underline;
}
.ir-artifacts {
  margin: 0;
  padding-left: 1rem;
}
.ir-artifacts li {
  margin: 0.2rem 0;
  font-family: var(--mono);
  font-size: 0.88rem;
}
.section-jumps {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin: 0 0 0.3rem;
}
.section-jumps a {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  padding: 0.35rem 0.6rem;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: rgba(255,255,255,0.6);
  color: var(--text);
  font-size: 0.82rem;
  text-decoration: none;
}
.section-jumps a:hover {
  text-decoration: none;
  border-color: rgba(182, 84, 31, 0.38);
}
.section-jumps .count {
  color: var(--muted);
}
details.collapsed-panel {
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(255,255,255,0.55);
  padding: 0.2rem 0.4rem;
}
details.collapsed-panel > summary {
  cursor: pointer;
  padding: 0.6rem 0.7rem;
  font-weight: 600;
  list-style: none;
}
details.collapsed-panel > summary::-webkit-details-marker { display: none; }
details.collapsed-panel > summary::before {
  content: "\u25B8  ";
  color: var(--muted);
}
details.collapsed-panel[open] > summary::before {
  content: "\u25BE  ";
}
details.collapsed-panel > .collapsed-body {
  padding: 0.2rem 0.5rem 0.6rem;
}
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
.sxs-wrap {
  border: 1px solid var(--line);
  border-radius: 14px;
  overflow: hidden;
  background: #fffdf9;
}
.sxs-head {
  display: grid;
  grid-template-columns: 1fr 1fr;
  border-bottom: 1px solid var(--line);
  background: rgba(191, 153, 121, 0.08);
}
.sxs-head-cell {
  padding: 0.55rem 0.9rem;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: var(--muted);
  font-family: var(--mono);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.sxs-head-cell + .sxs-head-cell { border-left: 1px solid var(--line); }
.sxs-diff {
  width: 100%;
  table-layout: fixed;
  border-collapse: collapse;
  font-family: var(--mono);
  font-size: 0.84rem;
  line-height: 1.45;
}
.sxs-diff colgroup col.ln { width: 3rem; }
.sxs-diff td { vertical-align: top; }
.sxs-diff td.sxs-ln {
  width: 3rem;
  padding: 0.12rem 0.55rem;
  color: var(--muted);
  text-align: right;
  background: rgba(191, 153, 121, 0.08);
  border-right: 1px solid var(--line);
  font-variant-numeric: tabular-nums;
  user-select: none;
  white-space: nowrap;
}
.sxs-diff td.sxs-text {
  padding: 0.14rem 0.85rem;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-wrap: anywhere;
}
.sxs-diff td.sxs-text + td.sxs-ln { border-left: 1px solid var(--line); }
.sxs-diff td.sxs-delete { background: rgba(143, 47, 29, 0.12); }
.sxs-diff td.sxs-delete.sxs-ln { background: rgba(143, 47, 29, 0.18); color: rgba(143, 47, 29, 0.85); }
.sxs-diff td.sxs-insert { background: rgba(38, 92, 67, 0.12); }
.sxs-diff td.sxs-insert.sxs-ln { background: rgba(38, 92, 67, 0.18); color: rgba(38, 92, 67, 0.85); }
.sxs-diff td.sxs-replace { background: rgba(153, 109, 0, 0.12); }
.sxs-diff td.sxs-blank { background: rgba(111, 90, 71, 0.05); color: transparent; }
.sxs-diff tr.sxs-equal td.sxs-text { color: var(--text); }
.sxs-diff tr.sxs-spacer td {
  padding: 0.55rem;
  text-align: center;
  background: rgba(191, 153, 121, 0.08);
  color: var(--muted);
  font-size: 0.72rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  border-top: 1px dashed var(--line);
  border-bottom: 1px dashed var(--line);
}
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
.workspace-shell {
  --sidebar-width: 20rem;
  min-height: 100vh;
  display: grid;
  grid-template-columns: var(--sidebar-width) 6px minmax(0, 1fr);
}
.sidebar {
  height: 100vh;
  position: sticky;
  top: 0;
  overflow-x: hidden;
  overflow-y: hidden;
  padding: 1rem 0.85rem 0.5rem;
  background: linear-gradient(180deg, rgba(255,255,255,0.88), rgba(255,250,242,0.78));
  backdrop-filter: blur(12px);
  min-width: 0;
  display: flex;
}
.sidebar > .stack,
.sidebar > .stack > * {
  min-width: 0;
}
.sidebar-toolbar {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.sidebar-search {
  width: 100%;
  padding: 0.5rem 0.7rem;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: rgba(255,255,255,0.7);
  font-size: 0.88rem;
}
.sidebar-toolbar-actions {
  display: flex;
  gap: 0.35rem;
  flex-wrap: wrap;
}
.sidebar-btn {
  padding: 0.3rem 0.6rem;
  font-size: 0.76rem;
  border-radius: 8px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.6);
  color: var(--muted);
  cursor: pointer;
}
.sidebar-btn:hover {
  color: var(--text);
  border-color: rgba(182, 84, 31, 0.4);
}
.sidebar-body {
  flex: 1 1 auto;
  min-height: 0;
  overflow-y: auto;
}
.tree {
  list-style: none;
  padding: 0;
  margin: 0;
  font-size: 0.86rem;
}
.tree-root { display: block; }
.tree-node { margin: 0; }
.tree-children {
  list-style: none;
  margin: 0;
  padding-left: 1.1rem;
  display: none;
}
.tree-node.open > .tree-children { display: block; }
.tree-row {
  display: grid;
  grid-template-columns: 1.1rem 1fr;
  align-items: center;
  column-gap: 0.25rem;
  padding: 0.18rem 0.35rem;
  border-radius: 6px;
  cursor: pointer;
}
.tree-row:hover { background: rgba(255, 255, 255, 0.65); }
.tree-node.node-artifact > .tree-row,
.tree-leaf { grid-template-columns: 1.35rem 1fr; }
.tree-toggle {
  width: 1.1rem;
  height: 1.1rem;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  margin: 0;
  border: none;
  background: transparent;
  color: var(--muted);
  cursor: pointer;
}
.tree-toggle:focus-visible { outline: 2px solid rgba(182, 84, 31, 0.5); outline-offset: 1px; border-radius: 4px; }
.tree-caret {
  width: 0;
  height: 0;
  border-top: 4px solid transparent;
  border-bottom: 4px solid transparent;
  border-left: 5px solid currentColor;
  transition: transform 140ms ease;
}
.tree-node.open > .tree-row > .tree-toggle > .tree-caret {
  transform: rotate(90deg);
}
.tree-label {
  display: grid;
  grid-template-columns: 1rem minmax(0, 1fr) auto;
  align-items: center;
  gap: 0.4rem;
  min-width: 0;
  color: inherit;
  text-decoration: none;
  padding: 0.05rem 0.2rem;
  border-radius: 5px;
}
.tree-label:hover { text-decoration: none; }
.tree-label.active {
  background: rgba(182, 84, 31, 0.14);
  font-weight: 600;
}
.tree-label-static { cursor: default; }
.tree-icon {
  font-size: 0.85rem;
  opacity: 0.6;
  text-align: center;
}
.tree-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
}
.tree-meta {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  flex-shrink: 0;
  font-size: 0.72rem;
  color: var(--muted);
}
.tree-meta .pill {
  padding: 0.08rem 0.4rem;
  font-size: 0.62rem;
  letter-spacing: 0.04em;
}
.tree-warn {
  color: var(--warn);
  font-size: 0.7rem;
  font-weight: 600;
}
.tree-run-sub {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.55rem;
  padding: 0 0.3rem 0.3rem 1.85rem;
  font-size: 0.72rem;
  color: var(--muted);
}
.tree-time {
  color: var(--text);
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.run-id-chip {
  font-size: 0.68rem;
  padding: 0.05rem 0.35rem;
  border-radius: 5px;
  background: rgba(191, 153, 121, 0.16);
  letter-spacing: 0.02em;
}
.tree-leaf {
  display: grid;
  grid-template-columns: 1.35rem 1fr;
  align-items: center;
  column-gap: 0.3rem;
  padding: 0.1rem 0.35rem;
  border-radius: 6px;
}
.tree-leaf:hover { background: rgba(255, 255, 255, 0.65); }
.tree-leaf.selected {
  background: rgba(182, 84, 31, 0.14);
}
.tree-leaf.selected .tree-name { font-weight: 600; }
.tree-check-wrap {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
}
.tree-check {
  width: 0.95rem;
  height: 0.95rem;
  accent-color: var(--accent);
  cursor: pointer;
}
.tree-leaf-label {
  display: grid;
  grid-template-columns: 1rem minmax(0, 1fr);
  align-items: center;
  gap: 0.35rem;
  min-width: 0;
  padding: 0.05rem 0.1rem;
  border-radius: 5px;
}
.tree-leaf-text {
  display: flex;
  flex-direction: column;
  min-width: 0;
  line-height: 1.2;
}
.tree-leaf-text .tree-name { font-size: 0.84rem; }
.tree-sub {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.tree-leaf.filter-hidden,
.tree-node.filter-hidden { display: none; }
.tree-empty {
  padding: 0.25rem 0.5rem;
  font-size: 0.78rem;
  color: var(--muted);
}
.compare-tray {
  position: sticky;
  bottom: 0;
  margin: 0 -0.5rem -0.5rem;
  padding: 0.75rem 0.85rem;
  border-top: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(255,253,248,0.96), rgba(248, 234, 220, 0.96));
  backdrop-filter: blur(8px);
  box-shadow: 0 -6px 16px rgba(73, 45, 20, 0.06);
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.compare-tray[hidden] { display: none; }
.compare-tray-label {
  font-size: 0.8rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
}
.compare-tray-preview {
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
  font-size: 0.82rem;
  font-family: var(--mono);
  color: var(--text);
  max-height: 5.5rem;
  overflow-y: auto;
}
.compare-tray-preview .item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.4rem;
  padding: 0.2rem 0.4rem;
  border-radius: 6px;
  background: rgba(255,255,255,0.5);
}
.compare-tray-preview .item .drop {
  cursor: pointer;
  border: none;
  background: transparent;
  color: var(--muted);
  font-size: 0.9rem;
  padding: 0;
}
.compare-tray-preview .item .drop:hover { color: var(--bad); }
.compare-tray-actions {
  display: flex;
  gap: 0.4rem;
}
.compare-btn {
  padding: 0.45rem 0.7rem;
  border-radius: 10px;
  border: 1px solid var(--line);
  background: linear-gradient(180deg, #fff9ef, #f2e4d6);
  color: var(--text);
  font-size: 0.88rem;
  cursor: pointer;
  flex: 1 1 auto;
}
.compare-btn:hover { border-color: rgba(182, 84, 31, 0.4); }
.compare-btn[disabled] {
  opacity: 0.55;
  cursor: not-allowed;
}
.compare-btn-ghost {
  background: transparent;
  flex: 0 0 auto;
  color: var(--muted);
}
.ctx-menu {
  position: fixed;
  z-index: 1000;
  min-width: 12rem;
  padding: 0.35rem;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #fffdf8;
  box-shadow: 0 20px 40px rgba(73, 45, 20, 0.18);
  display: flex;
  flex-direction: column;
  gap: 0.15rem;
}
.ctx-menu[hidden] { display: none; }
.ctx-item {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.7rem;
  padding: 0.45rem 0.6rem;
  border: none;
  border-radius: 6px;
  background: transparent;
  color: var(--text);
  font: inherit;
  font-size: 0.88rem;
  text-align: left;
  cursor: pointer;
}
.ctx-item:hover { background: rgba(191, 153, 121, 0.16); }
.ctx-item.ctx-danger { color: var(--bad); }
.ctx-item.ctx-danger:hover { background: rgba(143, 47, 29, 0.12); }
.ctx-hint { color: var(--muted); font-size: 0.76rem; }
.tree-node.node-run.ctx-target > .tree-row {
  outline: 2px solid rgba(182, 84, 31, 0.5);
  outline-offset: 2px;
}
.workspace-resizer {
  position: sticky;
  top: 0;
  height: 100vh;
  width: 6px;
  cursor: col-resize;
  background: var(--line);
  border-left: 1px solid var(--line);
  border-right: 1px solid var(--line);
  transition: background 120ms ease;
  user-select: none;
}
.workspace-resizer:hover,
.workspace-resizer.dragging {
  background: rgba(182, 84, 31, 0.55);
}
body.resizing,
body.resizing * {
  cursor: col-resize !important;
  user-select: none !important;
}
.sidebar > .stack {
  gap: 0.9rem;
}
.sidebar-inner {
  width: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
  gap: 0.6rem;
}
.sidebar-brand {
  padding: 0.2rem 0.15rem 0.25rem;
  flex: 0 0 auto;
}
.sidebar-brand a {
  color: inherit;
  text-decoration: none;
}
.sidebar-brand h1 {
  margin: 0;
  font-size: 1.4rem;
  letter-spacing: -0.05em;
}
.sidebar-brand p {
  margin: 0.35rem 0 0;
  color: var(--muted);
  font-size: 0.92rem;
  line-height: 1.4;
  overflow-wrap: anywhere;
}
.sidebar-label {
  margin: 0 0 0.6rem;
  color: var(--muted);
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.sidebar-card {
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.62);
  box-shadow: 0 10px 30px rgba(73, 45, 20, 0.05);
  padding: 0.95rem;
}
.sidebar-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.6rem;
}
.sidebar-stat {
  border: 1px solid rgba(93, 60, 32, 0.1);
  border-radius: 14px;
  padding: 0.7rem 0.75rem;
  background: rgba(255,255,255,0.52);
}
.sidebar-stat .label {
  display: block;
  color: var(--muted);
  font-size: 0.72rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.sidebar-stat .value {
  display: block;
  margin-top: 0.2rem;
  font-size: 1rem;
  font-weight: 700;
}
.run-list {
  display: grid;
  gap: 0.65rem;
}
.run-link {
  display: block;
  border: 1px solid rgba(93, 60, 32, 0.12);
  border-radius: 16px;
  padding: 0.8rem 0.85rem;
  color: inherit;
  background: rgba(255,255,255,0.56);
  transition: transform 120ms ease, border-color 120ms ease, box-shadow 120ms ease;
}
.run-link:hover {
  text-decoration: none;
  transform: translateY(-1px);
  border-color: rgba(182, 84, 31, 0.28);
  box-shadow: 0 10px 25px rgba(73, 45, 20, 0.08);
}
.run-link.active {
  border-color: rgba(182, 84, 31, 0.42);
  background: linear-gradient(180deg, rgba(255,253,248,0.96), rgba(248, 234, 220, 0.92));
  box-shadow: 0 16px 30px rgba(73, 45, 20, 0.08);
}
.run-top {
  display: flex;
  justify-content: space-between;
  align-items: start;
  gap: 0.6rem;
}
.run-id {
  font-size: 0.86rem;
  font-weight: 700;
  letter-spacing: -0.02em;
}
.run-command {
  margin-top: 0.35rem;
  color: var(--muted);
  font-family: var(--mono);
  font-size: 0.79rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.run-meta {
  margin-top: 0.55rem;
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
}
.run-meta span {
  border-radius: 999px;
  padding: 0.18rem 0.48rem;
  background: rgba(191, 153, 121, 0.12);
  color: var(--muted);
  font-size: 0.74rem;
}
.workspace {
  min-width: 0;
  display: flex;
  flex-direction: column;
}
.workspace-header {
  padding: 1.45rem 1.75rem 1rem;
  border-bottom: 1px solid var(--line);
  background: linear-gradient(180deg, rgba(255,255,255,0.9), rgba(255,255,255,0.58));
  backdrop-filter: blur(8px);
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 1rem;
}
.workspace-header > .workspace-heading {
  flex: 1 1 auto;
  min-width: 0;
}
.workspace-header h1 {
  margin: 0;
  font-size: clamp(1.7rem, 2.4vw, 2.45rem);
  letter-spacing: -0.05em;
  overflow-wrap: anywhere;
}
.workspace-header p {
  margin: 0.4rem 0 0;
  color: var(--muted);
  max-width: 84ch;
}
.workspace-header .actions {
  flex: 0 0 auto;
  margin-top: 0.35rem;
}
.workspace-subtitle {
  display: block;
  margin: 0.6rem 0 0;
  padding: 0.45rem 0.7rem;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.55);
  color: var(--muted);
  font-family: var(--mono);
  font-size: 0.86rem;
  line-height: 1.3;
  max-width: 100%;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.workspace-eyebrow {
  margin: 0 0 0.45rem;
  color: var(--muted);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.16em;
  text-transform: uppercase;
}
.workspace-body {
  min-width: 0;
  padding: 1.2rem 1.5rem 2rem;
}
.sidebar form.stack,
.sidebar form.inline {
  display: grid;
  gap: 0.6rem;
}
.sidebar label {
  display: grid;
  gap: 0.35rem;
  color: var(--muted);
  font-size: 0.85rem;
}
.sidebar select,
.sidebar input,
.sidebar button {
  width: 100%;
}
.sidebar .panel {
  padding: 0.9rem;
}
@media (max-width: 720px) {
  header.shell, main { padding-left: 1rem; padding-right: 1rem; }
  .title { flex-direction: column; align-items: start; }
  .workspace-header, .workspace-body, .sidebar {
    padding-left: 1rem;
    padding-right: 1rem;
  }
  .workspace-header {
    align-items: start;
  }
}
@media (max-width: 960px) {
  .workspace-shell {
    grid-template-columns: 1fr;
  }
  .sidebar {
    position: static;
    height: auto;
    border-bottom: 1px solid var(--line);
  }
  .workspace-resizer {
    display: none;
  }
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


COPY_BUTTON_SCRIPT = """
(() => {
  document.addEventListener('click', (event) => {
    const btn = event.target.closest('.copy-btn');
    if (!btn) return;
    const targetId = btn.dataset.copyTarget;
    if (!targetId) return;
    const source = document.getElementById(targetId);
    if (!source) return;
    const text = source.value ?? source.textContent ?? '';
    const done = () => {
      const original = btn.textContent;
      btn.textContent = 'Copied';
      btn.classList.add('copied');
      setTimeout(() => {
        btn.textContent = original;
        btn.classList.remove('copied');
      }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, () => {
        source.select();
        document.execCommand('copy');
        done();
      });
    } else {
      source.select();
      document.execCommand('copy');
      done();
    }
  });
})();
"""


HASH_OPEN_SCRIPT = """
(() => {
  const openFromHash = () => {
    const id = (location.hash || '').slice(1);
    if (!id) return;
    const el = document.getElementById(id);
    if (el && el.tagName === 'DETAILS') {
      el.open = true;
      el.scrollIntoView({ block: 'start', behavior: 'smooth' });
    }
  };
  window.addEventListener('hashchange', openFromHash);
  window.addEventListener('DOMContentLoaded', openFromHash);
})();
"""


CTX_MENU_SCRIPT = """
(() => {
  const menu = document.querySelector('[data-ctx-menu]');
  if (!menu) return;
  const base = (window.TLHUB_APP_BASE || '').replace(/\\/$/, '');
  let targetRunId = null;
  let targetNode = null;

  const hide = () => {
    menu.hidden = true;
    if (targetNode) targetNode.classList.remove('ctx-target');
    targetRunId = null;
    targetNode = null;
  };

  const show = (x, y, node, runId) => {
    hide();
    targetRunId = runId;
    targetNode = node;
    node.classList.add('ctx-target');
    menu.hidden = false;
    const rect = menu.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const left = Math.min(x, vw - rect.width - 8);
    const top = Math.min(y, vh - rect.height - 8);
    menu.style.left = Math.max(4, left) + 'px';
    menu.style.top = Math.max(4, top) + 'px';
  };

  document.addEventListener('contextmenu', (event) => {
    const node = event.target.closest('.tree-node.node-run');
    if (!node) { hide(); return; }
    const runId = node.dataset.runId;
    if (!runId) return;
    event.preventDefault();
    show(event.clientX, event.clientY, node, runId);
  });

  document.addEventListener('click', (event) => {
    if (menu.hidden) return;
    if (!menu.contains(event.target)) hide();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') hide();
  });
  window.addEventListener('scroll', hide, { passive: true, capture: true });
  window.addEventListener('resize', hide);

  menu.addEventListener('click', async (event) => {
    const btn = event.target.closest('[data-ctx-action]');
    if (!btn) return;
    event.stopPropagation();
    const action = btn.dataset.ctxAction;
    const runId = targetRunId;
    const node = targetNode;
    hide();
    if (action === 'delete-run' && runId) {
      const confirmed = window.confirm(`Delete run ${runId}?\\n\\nThis removes the database entry and trace files on disk.`);
      if (!confirmed) return;
      try {
        const url = `${base}/runs/${encodeURIComponent(runId)}/delete`;
        const response = await fetch(url, {
          method: 'POST',
          headers: { 'Accept': 'text/html' },
          redirect: 'manual',
        });
        const ok = response.ok || response.status === 0 || response.type === 'opaqueredirect';
        if (!ok && response.status !== 303) {
          throw new Error(`Server returned ${response.status}`);
        }
        // Drop the selection entries tied to this run.
        try {
          const SELECT_KEY = 'tlhub.compareSelection';
          const sel = JSON.parse(localStorage.getItem(SELECT_KEY) || '[]');
          const remaining = sel.filter((id) => !node.querySelector(`input.tree-check[value="${id}"]`));
          localStorage.setItem(SELECT_KEY, JSON.stringify(remaining));
        } catch (_e) {}
        const current = window.location.pathname || '';
        if (current.includes(`/runs/${runId}`) || current.includes(`/artifacts/`)) {
          window.location.href = base + '/';
        } else if (node && node.parentNode) {
          node.parentNode.removeChild(node);
        } else {
          window.location.reload();
        }
      } catch (err) {
        alert(`Failed to delete run: ${err.message || err}`);
      }
    }
  });
})();
"""


SIDEBAR_TREE_SCRIPT = """
(() => {
  const sidebar = document.querySelector('.sidebar-inner');
  if (!sidebar) return;

  const STATE_KEY = 'tlhub.treeOpen';
  const SELECT_KEY = 'tlhub.compareSelection';

  const loadJSON = (key, fallback) => {
    try {
      const value = JSON.parse(localStorage.getItem(key) || 'null');
      return value == null ? fallback : value;
    } catch (_err) {
      return fallback;
    }
  };
  const saveJSON = (key, value) => localStorage.setItem(key, JSON.stringify(value));

  // Tree open/close state (per tree-key).
  const openState = loadJSON(STATE_KEY, {});

  const applyOpenState = () => {
    sidebar.querySelectorAll('[data-tree-key]').forEach((toggle) => {
      const key = toggle.dataset.treeKey;
      const parent = toggle.closest('.tree-node');
      if (!parent) return;
      const shouldOpen = openState[key] || parent.classList.contains('open');
      parent.classList.toggle('open', !!shouldOpen);
      toggle.setAttribute('aria-expanded', shouldOpen ? 'true' : 'false');
      // Persist the current state so defaults (active run) are remembered too.
      if (shouldOpen) openState[key] = true;
    });
    saveJSON(STATE_KEY, openState);
  };

  sidebar.addEventListener('click', (event) => {
    const toggle = event.target.closest('.tree-toggle');
    if (!toggle || !sidebar.contains(toggle)) return;
    event.preventDefault();
    const key = toggle.dataset.treeKey;
    const parent = toggle.closest('.tree-node');
    const open = !parent.classList.contains('open');
    parent.classList.toggle('open', open);
    toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (open) openState[key] = true;
    else delete openState[key];
    saveJSON(STATE_KEY, openState);
  });

  sidebar.querySelectorAll('[data-tree-action]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.treeAction;
      sidebar.querySelectorAll('.tree-node').forEach((node) => {
        const toggle = node.querySelector(':scope > .tree-row > .tree-toggle');
        if (!toggle) return;
        const key = toggle.dataset.treeKey;
        const open = action === 'expand-all';
        node.classList.toggle('open', open);
        toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
        if (open) openState[key] = true;
        else delete openState[key];
      });
      saveJSON(STATE_KEY, openState);
    });
  });

  // Filtering.
  const search = sidebar.querySelector('.sidebar-search');
  if (search) {
    const filter = () => {
      const term = search.value.trim().toLowerCase();
      sidebar.querySelectorAll('.tree-leaf').forEach((leaf) => {
        const text = leaf.dataset.filterText || '';
        const match = !term || text.includes(term);
        leaf.classList.toggle('filter-hidden', !match);
      });
      // Hide empty branches (where all leaves are hidden). Keep nodes with hidden descendants visible only if term is empty or they still have a match.
      sidebar.querySelectorAll('.tree-node').forEach((node) => {
        if (!term) { node.classList.remove('filter-hidden'); return; }
        const hasVisibleLeaf = node.querySelector('.tree-leaf:not(.filter-hidden)');
        node.classList.toggle('filter-hidden', !hasVisibleLeaf);
        if (hasVisibleLeaf) {
          node.classList.add('open');
          const toggle = node.querySelector(':scope > .tree-row > .tree-toggle');
          if (toggle) toggle.setAttribute('aria-expanded', 'true');
        }
      });
    };
    search.addEventListener('input', filter);
  }

  // Selection tray.
  const tray = sidebar.querySelector('[data-compare-tray]');
  const countEl = sidebar.querySelector('[data-compare-count]');
  const previewEl = sidebar.querySelector('[data-compare-preview]');
  const goBtn = sidebar.querySelector('[data-compare-go]');
  const clearBtn = sidebar.querySelector('[data-compare-clear]');

  let selection = loadJSON(SELECT_KEY, []);
  const labelCache = {};

  const collectLabels = () => {
    sidebar.querySelectorAll('.tree-leaf input.tree-check').forEach((input) => {
      const id = input.value;
      const leaf = input.closest('.tree-leaf');
      const name = leaf?.querySelector('.tree-name')?.textContent?.trim() || id;
      labelCache[id] = name;
    });
  };

  const paintSelection = () => {
    const ids = new Set(selection);
    sidebar.querySelectorAll('.tree-leaf input.tree-check').forEach((input) => {
      const picked = ids.has(input.value);
      input.checked = picked;
      input.closest('.tree-leaf')?.classList.toggle('selected', picked);
    });
    renderTray();
  };

  const renderTray = () => {
    if (!tray) return;
    const count = selection.length;
    tray.hidden = count === 0;
    if (countEl) countEl.textContent = String(count);
    if (previewEl) {
      previewEl.innerHTML = selection.map((id, idx) => {
        const label = labelCache[id] || id;
        const role = idx === 0 ? 'Left' : idx === 1 ? 'Right' : `#${idx + 1}`;
        return `<div class="item"><span><strong>${role}:</strong> ${label}</span><button type="button" class="drop" data-remove="${id}" title="Remove">×</button></div>`;
      }).join('');
    }
    if (goBtn) goBtn.disabled = count !== 2;
  };

  sidebar.addEventListener('change', (event) => {
    const input = event.target.closest('input.tree-check');
    if (!input) return;
    const id = input.value;
    if (input.checked) {
      if (!selection.includes(id)) selection.push(id);
      if (selection.length > 2) {
        selection = selection.slice(-2);
      }
    } else {
      selection = selection.filter((x) => x !== id);
    }
    saveJSON(SELECT_KEY, selection);
    paintSelection();
  });

  if (previewEl) {
    previewEl.addEventListener('click', (event) => {
      const btn = event.target.closest('button.drop');
      if (!btn) return;
      const id = btn.dataset.remove;
      selection = selection.filter((x) => x !== id);
      saveJSON(SELECT_KEY, selection);
      paintSelection();
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      selection = [];
      saveJSON(SELECT_KEY, selection);
      paintSelection();
    });
  }

  if (goBtn) {
    goBtn.addEventListener('click', () => {
      if (selection.length !== 2) return;
      const [left, right] = selection;
      const base = window.TLHUB_APP_BASE || '';
      const url = `${base}/diff?left=${encodeURIComponent(left)}&right=${encodeURIComponent(right)}`;
      window.location.href = url;
    });
  }

  applyOpenState();
  collectLabels();
  paintSelection();
})();
"""


WORKSPACE_RESIZE_SCRIPT = """
(() => {
  const shell = document.querySelector('.workspace-shell');
  const handle = document.querySelector('.workspace-resizer');
  if (!shell || !handle) return;
  const STORAGE_KEY = 'tlhub.sidebarWidth';
  const MIN = 220;
  const MAX = 640;
  const clamp = (value) => Math.min(MAX, Math.max(MIN, value));
  const apply = (value) => {
    shell.style.setProperty('--sidebar-width', clamp(value) + 'px');
  };
  const saved = parseInt(localStorage.getItem(STORAGE_KEY) || '', 10);
  if (!Number.isNaN(saved)) apply(saved);
  let dragging = false;
  const onMove = (event) => {
    if (!dragging) return;
    const rect = shell.getBoundingClientRect();
    const width = clamp(event.clientX - rect.left);
    shell.style.setProperty('--sidebar-width', width + 'px');
  };
  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('dragging');
    document.body.classList.remove('resizing');
    const current = parseInt(getComputedStyle(shell).getPropertyValue('--sidebar-width'), 10);
    if (!Number.isNaN(current)) localStorage.setItem(STORAGE_KEY, String(current));
  };
  handle.addEventListener('mousedown', (event) => {
    event.preventDefault();
    dragging = true;
    handle.classList.add('dragging');
    document.body.classList.add('resizing');
  });
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
  handle.addEventListener('dblclick', () => {
    shell.style.setProperty('--sidebar-width', '20rem');
    localStorage.removeItem(STORAGE_KEY);
  });
  handle.addEventListener('keydown', (event) => {
    const step = event.shiftKey ? 32 : 8;
    const current = parseInt(getComputedStyle(shell).getPropertyValue('--sidebar-width'), 10) || 320;
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      apply(current - step);
      localStorage.setItem(STORAGE_KEY, String(clamp(current - step)));
    } else if (event.key === 'ArrowRight') {
      event.preventDefault();
      apply(current + step);
      localStorage.setItem(STORAGE_KEY, String(clamp(current + step)));
    }
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
    paths.daemon_version_path.write_text(__version__, encoding="utf-8")

    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        for path in (
            paths.daemon_pid_path,
            paths.daemon_port_path,
            paths.daemon_version_path,
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def make_handler(paths: TLHubPaths, repo: Repository) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = f"tlhub/{__version__}"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            try:
                path = strip_app_path_prefix(parsed.path)
            except ValueError as error:
                self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))
                return

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
            try:
                path = strip_app_path_prefix(parsed.path)
            except ValueError as error:
                self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) == 3 and parts[0] == "runs" and parts[2] == "delete":
                run_id = parts[1]
                repo.delete_run(run_id)
                shutil.rmtree(paths.runs_dir / run_id, ignore_errors=True)
                self.redirect(app_url("/"))
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


def app_url(
    path: str = "/",
    *,
    query: dict[str, Any] | list[tuple[str, Any]] | None = None,
) -> str:
    return build_app_url(path, query=query)


def render_workspace_page(
    repo: Repository,
    paths: TLHubPaths,
    *,
    page_title: str,
    heading: str,
    subtitle_html: str,
    content: str,
    selected_run_id: str | None = None,
    left_run: str | None = None,
    right_run: str | None = None,
    family: str = "",
    actions: str = "",
) -> str:
    sidebar = render_workspace_sidebar(
        repo,
        paths,
        selected_run_id=selected_run_id,
        left_run=left_run,
        right_run=right_run,
        family=family,
    )
    action_block = f"<div class='actions'>{actions}</div>" if actions else ""
    body = f"""
    <div class="workspace-shell">
      <aside class="sidebar">
        {sidebar}
      </aside>
      <div class="workspace-resizer" role="separator" aria-orientation="vertical" aria-label="Resize sidebar" tabindex="0"></div>
      <section class="workspace">
        <header class="workspace-header">
          <div class="workspace-heading">
            <div class="workspace-eyebrow">Visualizations</div>
            <h1>{escape(heading)}</h1>
            {subtitle_html if subtitle_html else ""}
          </div>
          {action_block}
        </header>
        <main class="workspace-body stack">
          {content}
        </main>
      </section>
    </div>
    <script>window.TLHUB_APP_BASE = {json.dumps(app_url('').rstrip('/'))};</script>
    <script>{WORKSPACE_RESIZE_SCRIPT}</script>
    <script>{HASH_OPEN_SCRIPT}</script>
    <script>{COPY_BUTTON_SCRIPT}</script>
    <script>{SIDEBAR_TREE_SCRIPT}</script>
    <script>{CTX_MENU_SCRIPT}</script>
    """
    return page(page_title, body)


def render_workspace_sidebar(
    repo: Repository,
    paths: TLHubPaths,
    *,
    selected_run_id: str | None = None,
    left_run: str | None = None,
    right_run: str | None = None,
    family: str = "",
) -> str:
    runs = repo.list_runs()
    active_run = selected_run_id or (runs[0]["id"] if runs else None)
    tree_html = (
        render_run_tree(repo, paths, runs, active_run)
        if runs
        else "<div class='empty'>No runs yet. Prefix any command with <code>tlhub</code> to capture traces.</div>"
    )
    return f"""
    <div class="sidebar-inner">
      <div class="sidebar-brand">
        <a href="{app_url('/')}">
          <h1>tlhub</h1>
        </a>
        <p>Local hub for <code>TORCH_TRACE</code> runs. Pick two artifacts anywhere below and hit compare.</p>
      </div>
      <div class="sidebar-toolbar">
        <input type="search" class="sidebar-search" placeholder="Filter by name, compile id, kind..." aria-label="Filter artifacts">
        <div class="sidebar-toolbar-actions">
          <button type="button" class="sidebar-btn" data-tree-action="expand-all" title="Expand all runs">Expand</button>
          <button type="button" class="sidebar-btn" data-tree-action="collapse-all" title="Collapse all runs">Collapse</button>
        </div>
      </div>
      <div class="sidebar-body">
        {tree_html}
      </div>
      <div class="compare-tray" data-compare-tray hidden>
        <div class="compare-tray-label"><span data-compare-count>0</span> selected</div>
        <div class="compare-tray-preview" data-compare-preview></div>
        <div class="compare-tray-actions">
          <button type="button" class="compare-btn" data-compare-go disabled>Compare</button>
          <button type="button" class="compare-btn compare-btn-ghost" data-compare-clear>Clear</button>
        </div>
      </div>
    </div>
    <div class="ctx-menu" data-ctx-menu role="menu" hidden>
      <button type="button" class="ctx-item ctx-danger" data-ctx-action="delete-run" role="menuitem">
        <span>Delete run</span>
        <span class="ctx-hint">and remove trace files</span>
      </button>
    </div>
    """


def render_run_tree(
    repo: Repository,
    paths: TLHubPaths,
    runs: list[dict[str, Any]],
    active_run_id: str | None,
) -> str:
    items = []
    for run in runs:
        items.append(render_run_tree_node(repo, paths, run, active_run_id))
    return f"<ul class='tree tree-root'>{''.join(items)}</ul>"


def compile_entry_route_key(compile_entry: dict[str, Any]) -> str:
    return str(
        compile_entry.get("entry_id")
        or compile_entry.get("compile_dir")
        or compile_entry.get("compile_id")
        or ""
    )


def compile_entry_url(run_id: str, compile_entry: dict[str, Any]) -> str:
    return app_url(
        "/runs/" + quote(run_id) + "/compiles/" + quote(compile_entry_route_key(compile_entry), safe="")
    )


def compile_entry_matches_scope(compile_entry: dict[str, Any], scope: dict[str, Any]) -> bool:
    if compile_entry.get("log_file") and scope.get("log_file"):
        if compile_entry.get("log_file") != scope.get("log_file"):
            return False
    if compile_entry.get("rank") is not None and scope.get("rank") is not None:
        if compile_entry.get("rank") != scope.get("rank"):
            return False

    compile_dir = compile_entry.get("compile_dir")
    scope_compile_dir = scope.get("compile_dir")
    if compile_dir and scope_compile_dir:
        return compile_dir == scope_compile_dir

    compile_id = compile_entry.get("compile_id")
    scope_compile_id = scope.get("compile_id")
    if compile_id and scope_compile_id:
        return compile_id == scope_compile_id
    return False


def find_compile_entry(
    manifest: dict[str, Any] | None,
    *,
    compile_key: str | None = None,
    scope: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    compiles = (manifest or {}).get("compiles", []) or []
    if compile_key:
        for entry in compiles:
            if compile_entry_route_key(entry) == compile_key:
                return entry
        for entry in compiles:
            if str(entry.get("compile_dir") or "") == compile_key:
                return entry
    if scope is not None:
        scoped_key = scope.get("compile_entry_id")
        if scoped_key:
            for entry in compiles:
                if compile_entry_route_key(entry) == str(scoped_key):
                    return entry
        for entry in compiles:
            if compile_entry_matches_scope(entry, scope):
                return entry
    return None


def collect_run_log_files(
    manifest: dict[str, Any],
    compiles: list[dict[str, Any]],
    primary_artifacts: list[dict[str, Any]],
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    has_unscoped = False

    def add(log_file: str | None) -> None:
        nonlocal has_unscoped
        value = str(log_file or "")
        if not value:
            has_unscoped = True
            return
        if value in seen:
            return
        seen.add(value)
        ordered.append(value)

    for log_file in manifest.get("log_files", []) or []:
        add(log_file)
    for compile_entry in compiles:
        add(compile_entry.get("log_file"))
    for artifact in primary_artifacts:
        add(artifact.get("log_file"))

    if has_unscoped:
        ordered.append("")
    return ordered


def render_log_tree_node(
    run_id: str,
    log_file: str,
    compile_entries: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
) -> str:
    children: list[str] = []
    for compile_entry in compile_entries:
        children.append(render_compile_tree_node(run_id, compile_entry))
    if artifacts:
        children.append(
            render_artifact_group(
                "Other",
                artifacts,
                group_key=f"log:{run_id}:{log_file or 'unscoped'}:other",
            )
        )

    ranks = sorted(
        {
            rank
            for rank in (
                *(entry.get("rank") for entry in compile_entries),
                *(artifact.get("rank") for artifact in artifacts),
            )
            if rank is not None
        }
    )
    artifact_count = sum(len(entry.get("artifacts") or []) for entry in compile_entries) + len(artifacts)
    meta_bits = []
    if len(ranks) == 1:
        meta_bits.append(f"rank {ranks[0]}")
    elif len(ranks) > 1:
        meta_bits.append(f"{len(ranks)} ranks")
    meta_bits.append(f"{len(compile_entries)}c")
    meta_bits.append(f"{artifact_count}f")
    label = Path(log_file).name if log_file else "Unscoped artifacts"
    tree_key = f"log:{run_id}:{log_file or 'unscoped'}"
    children_html = "".join(children) or "<li class='tree-empty muted'>No indexed artifacts.</li>"
    return f"""
    <li class="tree-node node-log" data-tree-node>
      <div class="tree-row">
        <button type="button" class="tree-toggle" data-tree-key="{escape(tree_key)}" aria-expanded="false">
          <span class="tree-caret" aria-hidden="true"></span>
        </button>
        <span class="tree-label tree-label-static" title="{escape(log_file or 'Artifacts without a source log file')}">
          <span class="tree-icon" aria-hidden="true">&#128220;</span>
          <span class="tree-name mono">{escape(label)}</span>
          <span class="tree-meta muted">{escape(' · '.join(meta_bits))}</span>
        </span>
      </div>
      <ul class="tree-children">{children_html}</ul>
    </li>
    """


def render_run_tree_node(
    repo: Repository,
    paths: TLHubPaths,
    run: dict[str, Any],
    active_run_id: str | None,
) -> str:
    run_id = run["id"]
    manifest = load_run_manifest(paths, run_id) or {}
    artifacts = repo.list_artifacts(run_id)
    artifact_by_id = {artifact["id"]: artifact for artifact in artifacts}
    report_ids = set(manifest.get("report_artifact_ids", []) or [])
    compiles = manifest.get("compiles", [])
    primary_artifacts = [artifact for artifact in artifacts if artifact["id"] not in report_ids]
    compile_artifact_ids = {
        artifact["id"]
        for compile_entry in compiles
        for artifact in (compile_entry.get("artifacts") or [])
    }

    compile_nodes = []
    log_files = collect_run_log_files(manifest, compiles, primary_artifacts)
    if len(log_files) > 1 and (not compiles or all(compile_entry.get("log_file") for compile_entry in compiles)):
        compiles_by_log: dict[str, list[dict[str, Any]]] = defaultdict(list)
        leftover_by_log: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for compile_entry in compiles:
            compiles_by_log[str(compile_entry.get("log_file") or "")].append(compile_entry)
        for artifact in primary_artifacts:
            if artifact["id"] in compile_artifact_ids:
                continue
            leftover_by_log[str(artifact.get("log_file") or "")].append(artifact)
        for log_file in log_files:
            compile_nodes.append(
                render_log_tree_node(
                    run_id,
                    log_file,
                    compiles_by_log.get(log_file, []),
                    leftover_by_log.get(log_file, []),
                )
            )
    else:
        for compile_entry in compiles:
            compile_nodes.append(render_compile_tree_node(run_id, compile_entry))

        leftover = [
            artifact
            for artifact in primary_artifacts
            if artifact["id"] not in compile_artifact_ids
        ]
        if leftover:
            compile_nodes.append(render_artifact_group("Other", leftover, group_key=f"run:{run_id}:other"))

    reports = [artifact_by_id[aid] for aid in report_ids if aid in artifact_by_id]
    if reports:
        compile_nodes.append(render_artifact_group("Reports", reports, group_key=f"run:{run_id}:reports"))

    children_html = "".join(compile_nodes) or "<li class='tree-empty muted'>No artifacts captured.</li>"
    is_active = run_id == active_run_id
    default_open = is_active
    expanded_attr = "true" if default_open else "false"
    open_class = " open" if default_open else ""

    warning_count = len(manifest.get("warnings", []) or [])
    warning_badge = f"<span class='tree-warn' title='{warning_count} warning(s)'>{warning_count}&#9888;</span>" if warning_count else ""
    summary_label = summarize_command(run)
    when_label = format_sidebar_time(run.get("started_at"))
    tooltip = f"{run_id}\n{run.get('command_display', '')}".strip()
    meta_parts = []
    if when_label:
        meta_parts.append(f"<span class='tree-time'>{escape(when_label)}</span>")
    meta_parts.append(f"<span class='muted'>{run['artifact_count']}f &middot; {len(compiles)}c</span>")
    if warning_badge:
        meta_parts.append(warning_badge)
    meta_parts.append(f"<span class='mono run-id-chip muted' title='{escape(run_id)}'>{escape(run_id[-8:])}</span>")
    sub_meta = "".join(meta_parts)
    return f"""
    <li class="tree-node node-run{open_class}" data-tree-node data-run-id="{escape(run_id)}">
      <div class="tree-row">
        <button type="button" class="tree-toggle" data-tree-key="run:{escape(run_id)}" aria-expanded="{expanded_attr}">
          <span class="tree-caret" aria-hidden="true"></span>
        </button>
        <a class="tree-label tree-run-label{' active' if is_active else ''}" href="{app_url('/runs/' + quote(run_id))}" title="{escape(tooltip)}">
          <span class="tree-icon" aria-hidden="true">&#128193;</span>
          <span class="tree-name">{escape(summary_label)}</span>
          <span class="tree-meta">{render_status(run['status'])}</span>
        </a>
      </div>
      <div class="tree-run-sub">{sub_meta}</div>
      <ul class="tree-children">{children_html}</ul>
    </li>
    """


def render_compile_tree_node(
    run_id: str,
    compile_entry: dict[str, Any],
) -> str:
    compile_dir = compile_entry.get("compile_dir") or ""
    compile_id = compile_entry.get("compile_id") or compile_dir
    artifacts = compile_entry.get("artifacts") or []
    status = compile_entry.get("status", "missing")
    leaves = "".join(render_artifact_leaf(artifact) for artifact in artifacts)
    leaves_html = leaves or "<li class='tree-empty muted'>No artifacts.</li>"
    compile_key = compile_entry_route_key(compile_entry)
    compile_link = compile_entry_url(run_id, compile_entry)
    return f"""
    <li class="tree-node node-compile" data-tree-node>
      <div class="tree-row">
        <button type="button" class="tree-toggle" data-tree-key="compile:{escape(run_id)}:{escape(compile_key)}" aria-expanded="false">
          <span class="tree-caret" aria-hidden="true"></span>
        </button>
        <a class="tree-label" href="{compile_link}">
          <span class="tree-icon" aria-hidden="true">&#9881;</span>
          <span class="tree-name mono">{escape(compile_id)}</span>
          <span class="tree-meta">
            <span class="pill {status_class(status)}">{escape(status)}</span>
            <span class="muted">{len(artifacts)} file{'s' if len(artifacts) != 1 else ''}</span>
          </span>
        </a>
      </div>
      <ul class="tree-children">{leaves_html}</ul>
    </li>
    """


def render_artifact_group(label: str, artifacts: list[dict[str, Any]], *, group_key: str) -> str:
    leaves = "".join(render_artifact_leaf(artifact) for artifact in artifacts)
    return f"""
    <li class="tree-node node-group" data-tree-node>
      <div class="tree-row">
        <button type="button" class="tree-toggle" data-tree-key="{escape(group_key)}" aria-expanded="false">
          <span class="tree-caret" aria-hidden="true"></span>
        </button>
        <span class="tree-label tree-label-static">
          <span class="tree-icon" aria-hidden="true">&#128196;</span>
          <span class="tree-name">{escape(label)}</span>
          <span class="tree-meta muted">{len(artifacts)}</span>
        </span>
      </div>
      <ul class="tree-children">{leaves}</ul>
    </li>
    """


def render_artifact_leaf(artifact: dict[str, Any]) -> str:
    filename = artifact["relative_path"].split("/")[-1]
    artifact_id = artifact["id"]
    kind = artifact.get("kind") or ""
    event = artifact.get("event_type") or ""
    subtitle_bits = [bit for bit in [kind, event] if bit and bit != kind]
    subtitle = " · ".join(subtitle_bits) if subtitle_bits else kind or ""
    return f"""
    <li class="tree-leaf" data-filter-text="{escape((filename + ' ' + kind + ' ' + event).lower())}">
      <label class="tree-check-wrap" title="Select to compare">
        <input type="checkbox" class="tree-check" value="{escape(artifact_id)}" aria-label="Select {escape(filename)} for comparison">
      </label>
      <a class="tree-label tree-leaf-label" href="{app_url('/artifacts/' + quote(artifact_id))}">
        <span class="tree-icon" aria-hidden="true">&#128196;</span>
        <span class="tree-leaf-text">
          <span class="tree-name mono">{escape(filename)}</span>
          {f"<span class='tree-sub muted'>{escape(subtitle)}</span>" if subtitle else ""}
        </span>
      </a>
    </li>
    """


def render_run_workspace_content(repo: Repository, paths: TLHubPaths, run_id: str) -> str:
    run = repo.get_run(run_id)
    manifest = load_run_manifest(paths, run_id)
    if run is None or manifest is None:
        return f"<div class='empty'>Run <code>{escape(run_id)}</code> is unavailable.</div>"

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
    failures_panel = render_failures_panel(manifest.get("failures_and_restarts", []))
    export_panel = render_export_panel(run_id, manifest.get("export"))
    multirank_panel = render_multi_rank_panel(manifest.get("multi_rank"))
    warnings_panel = render_warnings_panel(manifest.get("warnings", []))
    vllm_panel = render_vllm_panel(manifest.get("vllm"))
    provenance_panel = render_provenance_panel(run_id, provenance_groups, artifact_by_id)
    stack_trie = render_stack_trie(run_id, manifest.get("compiles", []))
    ir_dumps = render_ir_dumps_panel(run_id, manifest.get("compiles", []))
    unknown_stacks = render_unknown_stacks(manifest.get("unknown_stacks", []))
    report_panel = render_report_artifacts(report_artifacts)
    overview_panel = render_run_overview_panel(run, manifest)
    jumps = render_section_jumps(manifest, report_artifacts, provenance_groups, primary_artifacts)

    artifact_table = (
        ""
        if not primary_artifacts
        else f"""
        <div class="panel">
          <h2>All artifacts</h2>
          <p class="section-copy">Flat listing of every extracted per-event payload. Synthetic reports like <code>raw.jsonl</code> and multi-rank analyses are listed separately in Reports.</p>
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

    collapsed_sections = "".join(
        _collapsed(anchor, title, body)
        for anchor, title, body in [
            ("compile-directory", "Compile directory table", compile_table),
            ("artifacts", "All artifacts", artifact_table),
            ("reports", "Reports", report_panel),
            ("warnings", "Warnings", warnings_panel),
            ("failures", "Failures and restarts", failures_panel),
            ("export", "Export diagnostics", export_panel),
            ("multi-rank", "Multi-rank diagnostics", multirank_panel),
            ("vllm", "vLLM summary", vllm_panel),
            ("provenance", "Provenance tracking", provenance_panel),
            ("unknown-stacks", "Unknown stacks", unknown_stacks),
        ]
        if body
    )

    return f"""
    {overview_panel}
    {jumps}
    {stack_trie}
    {ir_dumps}
    {collapsed_sections}
    """


def _collapsed(anchor: str, title: str, body: str) -> str:
    return f"""
    <details class="collapsed-panel" id="{escape(anchor)}">
      <summary>{escape(title)}</summary>
      <div class="collapsed-body">{body}</div>
    </details>
    """


def render_section_jumps(
    manifest: dict[str, Any],
    report_artifacts: list[dict[str, Any]],
    provenance_groups: list[dict[str, Any]],
    primary_artifacts: list[dict[str, Any]],
) -> str:
    chips: list[tuple[str, str, int]] = []
    failures = manifest.get("failures_and_restarts", [])
    if failures:
        chips.append(("#failures", "Failures and restarts", len(failures)))
    if manifest.get("export"):
        chips.append(("#export", "Export diagnostics", len((manifest.get("export") or {}).get("failures", []) or [])))
    if manifest.get("multi_rank"):
        chips.append(("#multi-rank", "Multi-rank diagnostics", (manifest.get("multi_rank") or {}).get("num_ranks", 0)))
    if manifest.get("vllm"):
        chips.append(("#vllm", "vLLM summary", len((manifest.get("vllm") or {}).get("subgraphs", []) or [])))
    if provenance_groups:
        chips.append(("#provenance", "Provenance tracking", len(provenance_groups)))
    if manifest.get("warnings"):
        chips.append(("#warnings", "Warnings", len(manifest.get("warnings", []))))
    if report_artifacts:
        chips.append(("#reports", "Reports", len(report_artifacts)))
    if primary_artifacts:
        chips.append(("#artifacts", "All artifacts", len(primary_artifacts)))
    if not chips:
        return ""
    chip_html = "".join(
        f"<a href='{anchor}'>{escape(label)} <span class='count'>{count}</span></a>"
        for anchor, label, count in chips
    )
    return f"<div class='section-jumps'>{chip_html}</div>"


def render_run_overview_panel(run: dict[str, Any], manifest: dict[str, Any]) -> str:
    command = run.get("command_display") or ""
    command_block = (
        f"""
        <details class="command-block">
          <summary>Command</summary>
          <pre class="command-pre">{escape(command)}</pre>
        </details>
        """
        if command
        else ""
    )
    return f"""
    <div class="panel">
      <h2>Run overview</h2>
      <div class="kpis">
        <div class="kpi"><span class="label">Status</span><span class="value">{render_status(run['status'])}</span></div>
        <div class="kpi"><span class="label">Started</span><span class="value">{escape(format_timestamp(run['started_at']))}</span></div>
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
      {command_block}
    </div>
    """


def render_run_surface_panel(
    *,
    manifest: dict[str, Any],
    primary_artifacts: list[dict[str, Any]],
    report_artifacts: list[dict[str, Any]],
    provenance_groups: list[dict[str, Any]],
) -> str:
    kind_counts: dict[str, int] = {}
    for artifact in primary_artifacts:
        kind_counts[artifact["kind"]] = kind_counts.get(artifact["kind"], 0) + 1
    kind_preview = ", ".join(
        f"{count} {kind}"
        for kind, count in sorted(kind_counts.items(), key=lambda item: (-item[1], item[0]))[:4]
    ) or "n/a"
    graph_breaks = sum(1 for item in manifest.get("compiles", []) if item.get("status") == "break")
    failures = manifest.get("failures_and_restarts", [])
    export = manifest.get("export") or {}
    return f"""
    <div class="panel">
      <h2>Captured views</h2>
      <p class="section-copy">This pane keeps the run-level surfaces up front so you can move from compile orientation to detailed artifacts without switching screens.</p>
      <div class="summary-list">
        <div><span class="muted">Primary artifacts</span><span class="mono">{len(primary_artifacts)}</span></div>
        <div><span class="muted">Synthetic reports</span><span class="mono">{len(report_artifacts)}</span></div>
        <div><span class="muted">Provenance views</span><span class="mono">{len(provenance_groups)}</span></div>
        <div><span class="muted">Graph breaks</span><span class="mono">{graph_breaks}</span></div>
        <div><span class="muted">Failures or restarts</span><span class="mono">{len(failures)}</span></div>
        <div><span class="muted">Export diagnostics</span><span class="mono">{'captured' if export else 'none'}</span></div>
        <div><span class="muted">Multi-rank analysis</span><span class="mono">{'yes' if manifest.get('multi_rank') else 'no'}</span></div>
        <div><span class="muted">Artifact mix</span><span class="mono">{escape(kind_preview)}</span></div>
      </div>
    </div>
    """


def render_dashboard(repo: Repository, paths: TLHubPaths) -> str:
    runs = repo.list_runs()
    if not runs:
        content = "<div class='empty'>No runs captured yet. Prefix a command with <code>tlhub</code> to start collecting traces.</div>"
        return render_workspace_page(
            repo,
            paths,
            page_title="tlhub",
            heading="No runs yet",
            subtitle_html="<p>Wrap any command with <code>tlhub</code> and the daemon will index the trace automatically.</p>",
            content=content,
        )

    selected = runs[0]
    actions = (
        f"<a href='{app_url('/runs/' + quote(selected['id']))}'>Permalink</a>"
        f"<a href='{app_url('/compare', query={'left_run': selected['id']})}'>Compare run</a>"
    )
    return render_workspace_page(
        repo,
        paths,
        page_title="tlhub",
        heading=f"Run {selected['id']}",
        subtitle_html=render_command_subtitle(selected["command_display"]),
        content=render_run_workspace_content(repo, paths, selected["id"]),
        selected_run_id=selected["id"],
        left_run=selected["id"],
        actions=actions,
    )


def render_run_detail(repo: Repository, paths: TLHubPaths, run_id: str) -> str:
    run = repo.get_run(run_id)
    if run is None:
        return page("Run not found", f"<div class='empty'>Run <code>{escape(run_id)}</code> was not found.</div>")
    manifest = load_run_manifest(paths, run_id)
    if manifest is None:
        return page("Run not found", f"<div class='empty'>Run manifest for <code>{escape(run_id)}</code> is missing.</div>")
    delete_action = app_url("/runs/" + quote(run["id"]) + "/delete")
    actions = (
        f"<a href='{app_url('/compare', query={'left_run': run['id']})}'>Compare run</a>"
        f"<form class='inline' action='{delete_action}' method='post'>"
        "<button class='danger' type='submit'>Delete run</button>"
        "</form>"
    )
    return render_workspace_page(
        repo,
        paths,
        page_title=f"Run {run_id}",
        heading=f"Run {run['id']}",
        subtitle_html=render_command_subtitle(run["command_display"]),
        content=render_run_workspace_content(repo, paths, run_id),
        selected_run_id=run_id,
        left_run=run_id,
        actions=actions,
    )


def render_artifact(repo: Repository, paths: TLHubPaths, artifact_id: str) -> str:
    artifact = repo.get_artifact(artifact_id)
    if artifact is None:
        return page(
            "Artifact not found",
            f"<div class='empty'>Artifact <code>{escape(artifact_id)}</code> was not found.</div>",
        )

    run = repo.get_run(artifact["run_id"])
    assert run is not None
    manifest = load_run_manifest(paths, run["id"])
    content = read_artifact_text(paths, artifact)
    line_count = content.count("\n") + (0 if content.endswith("\n") or not content else 1)
    byte_count = len(content.encode("utf-8"))
    language = detect_language(artifact)
    summary = render_summary_card(artifact["summary"])
    source = render_source_block(content, language)
    compare_link = app_url(
        "/compare",
        query={"left_run": artifact["run_id"], "family": artifact["family"]},
    )
    compile_entry = find_compile_entry(manifest, scope=artifact)
    compile_id = artifact.get("compile_id") or (compile_entry or {}).get("compile_id")
    if compile_entry is not None:
        compile_link = f"<a href='{compile_entry_url(run['id'], compile_entry)}'>{escape(str(compile_id))}</a>"
    elif artifact.get("compile_dir"):
        compile_link = (
            f"<a href='{app_url('/runs/' + quote(run['id']) + '/compiles/' + quote(str(artifact['compile_dir']), safe=''))}'>"
            f"{escape(str(compile_id or artifact['compile_dir']))}</a>"
        )
    else:
        compile_link = "<span class='muted'>n/a</span>"
    filename = artifact["relative_path"].split("/")[-1]

    actions = (
        f"<a href='{compare_link}'>Compare family</a>"
        f"<a href='{app_url('/runs/' + quote(run['id']))}'>Back to run</a>"
    )

    subtitle = (
        "<div class='artifact-breadcrumb'>"
        f"<a href='{app_url('/runs/' + quote(run['id']))}'>Run {escape(run['id'])}</a>"
        f"<span class='sep'>/</span>"
        f"<span class='mono'>{escape(artifact['relative_path'])}</span>"
        "</div>"
    )

    language_label = {"python": "Python", "fx": "FX graph", "json": "JSON", "plain": "Text"}.get(language, "Text")
    content_panel = f"""
      <div class="panel artifact-content-panel">
        <div class="panel-head">
          <div>
            <h2>{escape(filename)}</h2>
            <div class="mini-note muted">{line_count} line{'s' if line_count != 1 else ''} · {format_bytes(byte_count)} · <span class='lang-chip'>{escape(language_label)}</span></div>
          </div>
          <div class="panel-head-actions">
            <button class="copy-btn" type="button" data-copy-target="artifact-text">Copy</button>
          </div>
        </div>
        <textarea id="artifact-text" class="visually-hidden" readonly>{escape(content)}</textarea>
        <div class="source-scroll">{source}</div>
      </div>
    """

    body = f"""
    <div class="grid-2">
      <div class="panel">
        <h2>Metadata</h2>
        <div class="summary-list">
          <div><span class="muted">Match key</span><span class="mono">{escape(artifact['match_key'])}</span></div>
          <div><span class="muted">Kind</span><span class="mono">{escape(artifact['kind'])}</span></div>
          <div><span class="muted">Event</span><span class="mono">{escape(artifact['event_type'])}</span></div>
          <div><span class="muted">Compile</span><span>{compile_link}</span></div>
          <div><span class="muted">Origin</span><span class="mono">{escape(f"{artifact['log_file']}:{artifact['line_no']}")}</span></div>
          <div><span class="muted">SHA</span><span class="mono">{escape(artifact['sha256'][:16])}</span></div>
        </div>
      </div>
      <div class="panel">
        <h2>Summary</h2>
        {summary}
      </div>
    </div>
    {content_panel}
    """
    return render_workspace_page(
        repo,
        paths,
        page_title=artifact["title"],
        heading=artifact["title"],
        subtitle_html=subtitle,
        content=body,
        selected_run_id=run["id"],
        left_run=run["id"],
        family=artifact["family"],
        actions=actions,
    )


def render_compare(repo: Repository, params: dict[str, list[str]]) -> str:
    runs = repo.list_runs()
    left_run_id = first_param(params, "left_run")
    right_run_id = first_param(params, "right_run")
    family_filter = first_param(params, "family") or ""

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
    return render_workspace_page(
        repo,
        repo.paths if hasattr(repo, "paths") else ensure_layout(None),
        page_title="Compare runs",
        heading="Compare runs",
        subtitle_html="<p>Match artifacts by family and occurrence order, or pick any two artifacts manually when the pairing is not obvious.</p>",
        content=content if content else "<div class='empty'>Choose two runs in the left rail to line up their artifacts.</div>",
        selected_run_id=left_run_id,
        left_run=left_run_id,
        right_run=right_run_id,
        family=family_filter,
    )


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

    diff_table = render_side_by_side_diff(
        left_text,
        right_text,
        left_title=left["title"],
        right_title=right["title"],
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
    semantic_diff = render_semantic_diff(left, right, left_text, right_text)
    compare_query = {
        "left_run": left["run_id"],
        "right_run": right["run_id"],
    }
    if left["family"] == right["family"]:
        compare_query["family"] = left["family"]
    content = f"""
    <div class="grid-2">
      {render_diff_side("Left", left)}
      {render_diff_side("Right", right)}
    </div>
    {semantic_diff}
    <div class="panel">
      <h2>Side-by-side diff</h2>
      {diff_table}
    </div>
    <div class="panel">
      <details class="unified">
        <summary>Unified diff</summary>
        <pre class="unified">{escape(unified or 'No textual diff.')}</pre>
      </details>
    </div>
    """
    return render_workspace_page(
        repo,
        paths,
        page_title="Diff",
        heading="Artifact diff",
        subtitle_html=(
            f"<p><span class='mono'>{escape(left['match_key'])}</span> "
            f"vs <span class='mono'>{escape(right['match_key'])}</span></p>"
        ),
        content=content,
        selected_run_id=left["run_id"],
        left_run=left["run_id"],
        right_run=right["run_id"],
        family=left["family"] if left["family"] == right["family"] else "",
        actions=f"<a href='{app_url('/compare', query=compare_query)}'>Back to compare</a>",
    )


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

    compile_entry = find_compile_entry(manifest, compile_key=compile_key)
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
    backward_metrics_panel = render_backward_metrics_panel(
        compile_entry.get("bwd_compilation_metrics") or {},
        compile_entry.get("aot_autograd_backward_compilation_metrics") or {},
    )
    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(compile_entry['compile_id'])}</h1>
          <p><a href="{app_url('/runs/' + quote(run_id))}">Run {escape(run_id)}</a> | <span class="mono">{escape(compile_entry['compile_dir'])}</span></p>
        </div>
        <div class="actions">
          <a href="{app_url('/runs/' + quote(run_id))}">Back to run</a>
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
            <div><span class="muted">Log file</span><span class="mono">{escape(str(compile_entry.get('log_file') or 'n/a'))}</span></div>
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
      {backward_metrics_panel}
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


def render_backward_metrics_panel(
    bwd: dict[str, Any],
    aot_backward: dict[str, Any],
) -> str:
    if not bwd and not aot_backward:
        return ""
    bwd_time_rows = render_metrics_list(
        {
            "Inductor compile time (s)": bwd.get("inductor_compile_time_s"),
            "Codegen time (s)": bwd.get("code_gen_time_s"),
        }
    )
    bwd_failures = (
        f"<div class='summary-list'>"
        f"<div><span class='muted'>Failure type</span><span class='mono'>{escape(str(bwd.get('fail_type')))}</span></div>"
        f"<div><span class='muted'>Reason</span><span class='mono'>{escape(str(bwd.get('fail_reason') or ''))}</span></div>"
        f"</div>"
        if bwd.get("fail_type")
        else "<div class='muted'>No failures.</div>"
    )
    aot_failures = (
        f"<div class='summary-list'>"
        f"<div><span class='muted'>Failure type</span><span class='mono'>{escape(str(aot_backward.get('fail_type')))}</span></div>"
        f"<div><span class='muted'>Reason</span><span class='mono'>{escape(str(aot_backward.get('fail_reason') or ''))}</span></div>"
        f"</div>"
        if aot_backward.get("fail_type")
        else "<div class='muted'>No failures.</div>"
    )
    return f"""
    <div class="grid-2">
      <div class="panel">
        <h2>Backward compilation metrics</h2>
        <p class="section-copy">Mirrors the <code>bwd_compilation_metrics.html</code> surface in <code>tlparse</code>: backward-phase compile times and failures.</p>
        {bwd_time_rows if bwd else "<div class='muted'>No backward compile metrics captured.</div>"}
        <h3 style="margin-top:1rem;">Failures</h3>
        {bwd_failures}
      </div>
      <div class="panel">
        <h2>AOT Autograd backward</h2>
        <p class="section-copy">Mirrors the <code>aot_autograd_backward_compilation_metrics.html</code> surface in <code>tlparse</code>: AOT Autograd backward compilation failures.</p>
        {aot_failures}
      </div>
    </div>
    """


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
    compile_entry = find_compile_entry(manifest, scope=guard)
    compile_href = compile_entry_url(run_id, compile_entry) if compile_entry is not None else app_url("/runs/" + quote(run_id))
    compile_label = escape(str(guard.get("compile_id") or "compile"))

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(guard.get('failure_type') or 'Guard detail')}</h1>
          <p><a href="{app_url('/runs/' + quote(run_id))}">Run {escape(run_id)}</a> | <a href="{compile_href}">{compile_label}</a></p>
        </div>
        <div class="actions">
          <a href="{app_url('/runs/' + quote(run_id))}">Back to run</a>
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
              <td><a href="{app_url('/runs/' + quote(run_id) + '/provenance/' + quote(group['id']))}">{escape(group['label'])}</a></td>
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
        f"<a href='{app_url('/artifacts/' + quote(aot_code_artifact['id']))}'>Open AOT wrapper artifact</a>"
        if code_mode == "python" and aot_code_artifact is not None
        else (
            f"<a href='{app_url('/artifacts/' + quote(output_code_artifact['id']))}'>Open Python output artifact</a>"
            if code_mode == "cpp" and output_code_artifact is not None
            else ""
        )
    )
    extra_links = (
        "".join(
            f"<li><a href='{app_url('/artifacts/' + quote(artifact['id']))}'>{escape(artifact['title'])}</a></li>"
            for artifact in extra_artifacts
        )
        if extra_artifacts
        else "<li class='muted'>No supplemental provenance artifacts.</li>"
    )
    interactive_mappings_json = json.dumps(interactive_mappings, sort_keys=True).replace("</", "<\\/")

    body = f"""
    <header class="shell">
      <div class="title">
        <div>
          <h1>{escape(group['label'])}</h1>
          <p><a href="{app_url('/runs/' + quote(run_id))}">Run {escape(run_id)}</a> | <span class="mono">{escape(str(group.get('compile_id') or group.get('compile_dir') or provenance_id))}</span></p>
        </div>
        <div class="actions">
          <a href="{app_url('/runs/' + quote(run_id))}">Back to run</a>
        </div>
      </div>
    </header>
    <main class="stack">
      <style>{PROVENANCE_STYLE}</style>
      <script id="provenanceMappings" type="application/json">{interactive_mappings_json}</script>
      <div class="grid-2">
        <div class="panel">
          <h2>Mapping summary</h2>
          {render_metrics_list(mapping_counts)}
          <div class="summary-list" style="margin-top:1rem;">
            <div><span class="muted">Rank</span><span class="mono">{escape(str(group.get('rank') if group.get('rank') is not None else 'n/a'))}</span></div>
            <div><span class="muted">Log file</span><span class="mono">{escape(str(group.get('log_file') or 'n/a'))}</span></div>
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
    return f"<a href='{app_url('/artifacts/' + quote(artifact['id']))}'>{escape(text)}</a>"


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
          <td><a href="{app_url('/artifacts/' + quote(artifact['id']))}">{escape(artifact['title'])}</a></td>
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
              <td class="mono"><a href="{compile_entry_url(run_id, compile_entry)}">{escape(compile_entry['compile_id'])}</a></td>
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
            f"<a href='{app_url('/runs/' + quote(run_id) + '/guards/' + quote(detail_id))}'>details</a>"
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
        f"<a href='{app_url('/artifacts/' + quote(exported_program_artifact_id))}'>View exported program</a>"
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
        f"<a href='{app_url('/artifacts/' + quote(piecewise['id']))}'>View piecewise split graph</a>"
        if piecewise and piecewise.get("id")
        else "<span class='muted'>No piecewise split graph artifact</span>"
    )
    subgraph_blocks = []
    for subgraph in subgraphs:
        artifacts = subgraph.get("artifacts") or []
        artifact_links = (
            "".join(
                f"<li><a href='{app_url('/artifacts/' + quote(artifact['id']))}'>{escape(artifact['title'])}</a></li>"
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
    body = (
        f"<div class='stack-tree'>{render_stack_tree_children(run_id, tree['children'])}</div>"
        if tree["children"]
        else "<div class='empty'>No compile stacks were captured in this run.</div>"
    )
    legend = (
        "<div class='stack-legend'>"
        "<span class='pill status-ok'>Success</span>"
        "<span class='pill status-break'>Restart</span>"
        "<span class='pill status-empty'>Empty graph</span>"
        "<span class='pill status-error'>Error</span>"
        "<span class='pill status-missing'>Metrics missing</span>"
        "</div>"
    )
    return f"""
    <div class="panel">
      <h2>Stack trie</h2>
      <p class="section-copy">A tree of stack frames for every stack that triggered PT2 compilation (most recent call last). Click a compile id pill to jump to its IR dumps and metrics.</p>
      {legend}
      {body}
    </div>
    """


def render_ir_dumps_panel(run_id: str, compiles: list[dict[str, Any]]) -> str:
    if not compiles:
        return ""
    items = []
    for compile_entry in compiles:
        compile_dir = compile_entry.get("compile_dir") or ""
        compile_id = compile_entry.get("compile_id") or compile_dir
        status = compile_entry.get("status", "missing")
        artifacts = compile_entry.get("artifacts") or []
        artifact_list = (
            "<div class='muted mini-note'>No artifacts emitted for this compile.</div>"
            if not artifacts
            else "<ul class='mono-list ir-artifacts'>" + "".join(
                f"<li><a href='{app_url('/artifacts/' + quote(artifact['id']))}'>"
                f"{escape(artifact['relative_path'].split('/')[-1])}</a>"
                f" <span class='mini-note muted'>({escape(str(artifact.get('event_type') or ''))})</span></li>"
                for artifact in artifacts
            ) + "</ul>"
        )
        compile_link = compile_entry_url(run_id, compile_entry)
        items.append(
            f"""
            <li class="ir-compile">
              <div class="ir-compile-head">
                <a class="ir-compile-id pill {status_class(status)}" href="{compile_link}">{escape(compile_id)}</a>
                <span class="muted mini-note">{len(artifacts)} file{'s' if len(artifacts) != 1 else ''}</span>
              </div>
              {artifact_list}
            </li>
            """
        )
    return f"""
    <div class="panel">
      <h2>IR dumps</h2>
      <p class="section-copy">Every compile id, with the intermediate products it emitted. Click a compile id for metrics, stack, and provenance; click a file to open it directly.</p>
      <ul class="ir-list">{''.join(items)}</ul>
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
                "entry_id": compile_entry.get("entry_id"),
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
            f" <a class='pill {status_class(item.get('status'))}' href='{compile_entry_url(run_id, item)}'>{escape(item['compile_id'])}</a>"
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
        f"<li><a href='{app_url('/artifacts/' + quote(artifact['id']))}'>{escape(artifact['relative_path'].split('/')[-1])}</a> <span class='mini-note'>({artifact['number']})</span></li>"
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
        f"<li><a href='{app_url('/runs/' + quote(run_id) + '/guards/' + quote(guard_id))}'>{escape(guard_id)}</a></li>"
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
        if compile_entry_matches_scope(compile_entry, group)
    ]
    if not matches:
        return ""
    rows = "".join(
        f"<li><a href='{app_url('/runs/' + quote(run_id) + '/provenance/' + quote(group['id']))}'>{escape(group['label'])}</a></li>"
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
      <form action="{app_url('/compare')}" method="get" class="toolbar">
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
      <form action="{app_url('/diff')}" method="get" class="stack">
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
            action = f"<a href='{app_url('/diff', query={'left': left['id'], 'right': right['id']})}'>Diff</a>"
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
    compare_url = app_url("/compare", query={"left_run": run["id"]})
    delete_action = app_url("/runs/" + quote(run["id"]) + "/delete")
    compile_count = manifest.get("compile_count", 0) if manifest else 0
    rank_count = format_rank_count(manifest.get("multi_rank") if manifest else None)
    return f"""
    <tr>
      <td class="mono"><a href="{app_url('/runs/' + quote(run['id']))}">{escape(run['id'])}</a></td>
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
          <a href="{app_url('/runs/' + quote(run['id']))}">View</a>
          <a href="{compare_url}">Compare</a>
          <form class="inline" action="{delete_action}" method="post">
            <button class="danger" type="submit">Delete</button>
          </form>
        </div>
      </td>
    </tr>
    """


def render_artifact_row(artifact: dict[str, Any]) -> str:
    compare_url = app_url(
        "/compare",
        query={"left_run": artifact["run_id"], "family": artifact["family"]},
    )
    origin = f"{artifact['log_file']}:{artifact['line_no']}"
    if artifact["compile_id"]:
        origin += f" | {artifact['compile_id']}"
    return f"""
    <tr>
      <td><a href="{app_url('/artifacts/' + quote(artifact['id']))}">{escape(artifact['title'])}</a></td>
      <td class="mono">{escape(artifact['match_key'])}</td>
      <td>{escape(artifact['kind'])}</td>
      <td class="mono">{escape(origin)}</td>
      <td>{escape(format_artifact_summary(artifact['summary']))}</td>
      <td>
        <div class="actions">
          <a href="{app_url('/artifacts/' + quote(artifact['id']))}">View</a>
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
        f"<a href='{app_url('/artifacts/' + quote(artifact['id']))}'>{escape(artifact['title'])}</a>"
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
        <div><span class="muted">Run</span><span class="mono"><a href="{app_url('/runs/' + quote(artifact['run_id']))}">{escape(artifact['run_id'])}</a></span></div>
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


def render_side_by_side_diff(
    left_text: str,
    right_text: str,
    *,
    left_title: str = "Left",
    right_title: str = "Right",
    context: int = 3,
) -> str:
    left_lines = left_text.splitlines()
    right_lines = right_text.splitlines()
    matcher = difflib.SequenceMatcher(a=left_lines, b=right_lines, autojunk=False)
    opcodes = matcher.get_opcodes()
    if not opcodes:
        return "<div class='empty'>Both files are empty.</div>"

    rows: list[str] = []
    any_change = any(tag != "equal" for tag, *_ in opcodes)
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            length = i2 - i1
            if any_change and length > context * 2 + 1:
                for k in range(context):
                    rows.append(_sxs_row("equal", i1 + k, j1 + k, left_lines[i1 + k], right_lines[j1 + k]))
                rows.append(_sxs_spacer(length - context * 2))
                start = length - context
                for k in range(context):
                    rows.append(
                        _sxs_row(
                            "equal",
                            i1 + start + k,
                            j1 + start + k,
                            left_lines[i1 + start + k],
                            right_lines[j1 + start + k],
                        )
                    )
            else:
                for k in range(length):
                    rows.append(_sxs_row("equal", i1 + k, j1 + k, left_lines[i1 + k], right_lines[j1 + k]))
        elif tag == "replace":
            left_block = left_lines[i1:i2]
            right_block = right_lines[j1:j2]
            max_len = max(len(left_block), len(right_block))
            for k in range(max_len):
                l_line = left_block[k] if k < len(left_block) else None
                r_line = right_block[k] if k < len(right_block) else None
                rows.append(
                    _sxs_row(
                        "replace",
                        (i1 + k) if l_line is not None else None,
                        (j1 + k) if r_line is not None else None,
                        l_line if l_line is not None else "",
                        r_line if r_line is not None else "",
                        left_kind="delete" if l_line is not None else "blank",
                        right_kind="insert" if r_line is not None else "blank",
                    )
                )
        elif tag == "delete":
            for k in range(i2 - i1):
                rows.append(
                    _sxs_row(
                        "delete",
                        i1 + k,
                        None,
                        left_lines[i1 + k],
                        "",
                        left_kind="delete",
                        right_kind="blank",
                    )
                )
        elif tag == "insert":
            for k in range(j2 - j1):
                rows.append(
                    _sxs_row(
                        "insert",
                        None,
                        j1 + k,
                        "",
                        right_lines[j1 + k],
                        left_kind="blank",
                        right_kind="insert",
                    )
                )

    if not any_change:
        return "<div class='empty'>Files are identical.</div>"

    rows_html = "".join(rows)
    return f"""
    <div class="sxs-wrap">
      <div class="sxs-head">
        <div class="sxs-head-cell">{escape(left_title)}</div>
        <div class="sxs-head-cell">{escape(right_title)}</div>
      </div>
      <table class="sxs-diff"><tbody>{rows_html}</tbody></table>
    </div>
    """


def _sxs_row(
    tag: str,
    left_num: int | None,
    right_num: int | None,
    left_text: str,
    right_text: str,
    *,
    left_kind: str | None = None,
    right_kind: str | None = None,
) -> str:
    left_kind = left_kind or tag
    right_kind = right_kind or tag
    l_num = "" if left_num is None else str(left_num + 1)
    r_num = "" if right_num is None else str(right_num + 1)
    return (
        f"<tr class='sxs-row sxs-{escape(tag)}'>"
        f"<td class='sxs-ln sxs-ln-l sxs-{escape(left_kind)}'>{l_num}</td>"
        f"<td class='sxs-text sxs-{escape(left_kind)}'>{escape(left_text) if left_text else '&nbsp;'}</td>"
        f"<td class='sxs-ln sxs-ln-r sxs-{escape(right_kind)}'>{r_num}</td>"
        f"<td class='sxs-text sxs-{escape(right_kind)}'>{escape(right_text) if right_text else '&nbsp;'}</td>"
        f"</tr>"
    )


def _sxs_spacer(skipped: int) -> str:
    return (
        f"<tr class='sxs-spacer'>"
        f"<td colspan='4'>&middot; &middot; &middot; {skipped} unchanged line{'s' if skipped != 1 else ''} &middot; &middot; &middot;</td>"
        f"</tr>"
    )


def render_source_block(text: str, language: str = "plain") -> str:
    lines = text.splitlines() or [""]
    rows = "".join(
        f"<tr id='L{index}'><td class='ln'><a href='#L{index}'>{index}</a></td>"
        f"<td class='code'><code class='hl-{escape(language)}'>{_highlight_line(line, language)}</code></td></tr>"
        for index, line in enumerate(lines, start=1)
    )
    return f"<table class='source'><tbody>{rows}</tbody></table>"


_PY_KEYWORDS = frozenset(
    {
        "and", "as", "assert", "async", "await", "break", "class", "continue",
        "def", "del", "elif", "else", "except", "finally", "for", "from",
        "global", "if", "import", "in", "is", "lambda", "nonlocal", "not",
        "or", "pass", "raise", "return", "try", "while", "with", "yield",
        "match", "case",
    }
)
_PY_CONSTANTS = frozenset({"True", "False", "None", "self", "cls"})
_PY_BUILTINS = frozenset(
    {
        "print", "len", "range", "enumerate", "zip", "map", "filter", "list",
        "dict", "tuple", "set", "int", "float", "str", "bool", "bytes",
        "object", "type", "isinstance", "issubclass", "hasattr", "getattr",
        "setattr", "repr", "super", "abs", "min", "max", "sum", "any", "all",
        "iter", "next", "sorted", "reversed", "round", "open", "input",
    }
)

_PY_TOKEN_RE = re.compile(
    r"(?P<comment>\#[^\n]*)"
    r"|(?P<string>r?b?\"(?:\\.|[^\"\\])*\"|r?b?'(?:\\.|[^'\\])*')"
    r"|(?P<number>\b(?:0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d[\d_]*)?j?)\b)"
    r"|(?P<decor>@[A-Za-z_][\w.]*)"
    r"|(?P<fxname>%[A-Za-z_]\w*)"
    r"|(?P<name>\b[A-Za-z_]\w*\b)"
    r"|(?P<op>[+\-*/%=<>!&|^~]+|->)"
)

_JSON_TOKEN_RE = re.compile(
    r"(?P<string>\"(?:\\.|[^\"\\])*\")"
    r"|(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
    r"|(?P<keyword>\b(?:true|false|null)\b)"
    r"|(?P<punct>[\{\}\[\],:])"
)


def _highlight_line(line: str, language: str) -> str:
    if not line:
        return "&nbsp;"
    if language == "python" or language == "fx":
        return _apply_regex(line, _PY_TOKEN_RE, language)
    if language == "json":
        return _apply_regex(line, _JSON_TOKEN_RE, language)
    return escape(line)


def _apply_regex(line: str, regex: re.Pattern[str], language: str) -> str:
    out: list[str] = []
    pos = 0
    for match in regex.finditer(line):
        if match.start() > pos:
            out.append(escape(line[pos : match.start()]))
        token = match.group()
        group = match.lastgroup or ""
        cls = group
        if group == "name":
            if token in _PY_KEYWORDS:
                cls = "keyword"
            elif token in _PY_CONSTANTS:
                cls = "const"
            elif token in _PY_BUILTINS:
                cls = "builtin"
            elif match.end() < len(line) and line[match.end()] == "(":
                cls = "func"
        elif group == "fxname":
            cls = "fxname"
        elif group == "decor":
            cls = "decor"
        out.append(f"<span class='hl-{cls}'>{escape(token)}</span>")
        pos = match.end()
    if pos < len(line):
        out.append(escape(line[pos:]))
    return "".join(out)


def detect_language(artifact: dict[str, Any]) -> str:
    kind = (artifact.get("kind") or "").lower()
    relative_path = (artifact.get("relative_path") or "").lower()
    if kind == "inductor_output_code" or relative_path.endswith(".py"):
        return "python"
    if kind in {"json", "manifest_json"} or relative_path.endswith(".json"):
        return "json"
    if kind == "fx_graph" or "graph" in kind:
        return "fx"
    return "plain"


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


def render_command_subtitle(command: str) -> str:
    collapsed = " ".join(command.split())
    return (
        f"<div class='workspace-subtitle' title='{escape(command)}'>"
        f"{escape(collapsed)}"
        "</div>"
    )


def format_timestamp(value: str | None) -> str:
    if not value:
        return "n/a"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return parsed.strftime("%b %d, %Y %H:%M:%S")


def format_sidebar_time(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return ""
    now = datetime.now(parsed.tzinfo) if parsed.tzinfo else datetime.now()
    if parsed.date() == now.date():
        return parsed.strftime("today %H:%M")
    delta = now - parsed
    if delta.days < 7 and delta.days >= 0:
        return parsed.strftime("%a %H:%M")
    return parsed.strftime("%b %d %H:%M")


def summarize_command(run: dict[str, Any]) -> str:
    command = run.get("command") or []
    if not command:
        display = run.get("command_display") or run.get("id") or ""
        return trim(display, 48)

    first = str(command[0]).rsplit("/", 1)[-1] or str(command[0])
    rest = list(command[1:])

    if first.startswith("python") or first in {"python", "uv", "pytest", "pipx", "pip"}:
        i = 0
        while i < len(rest):
            arg = str(rest[i])
            if arg.endswith(".py") or arg.endswith(".ipynb"):
                return arg.rsplit("/", 1)[-1]
            if arg == "-m" and i + 1 < len(rest):
                return f"-m {rest[i + 1]}"
            if arg == "-c" and i + 1 < len(rest):
                snippet = str(rest[i + 1]).splitlines()[0] if rest[i + 1] else ""
                return f"-c {trim(snippet, 36)}"
            i += 1

    pieces = [first] + [str(arg).rsplit("/", 1)[-1] for arg in rest[:2]]
    return trim(" ".join(part for part in pieces if part), 48)


def format_bytes(count: int) -> str:
    if count < 1024:
        return f"{count} B"
    if count < 1024 * 1024:
        return f"{count / 1024:.1f} KB"
    return f"{count / (1024 * 1024):.1f} MB"


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

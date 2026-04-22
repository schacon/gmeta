#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPEC_DIR = ROOT / "spec"
# The generated spec sub-site lives at docs/spec/ (served from
# https://git-meta.com/spec/). The marketing landing page and other
# hand-curated assets at docs/ are intentionally outside the generator's
# blast radius — the only files this script writes outside docs/spec/ are
# the site-wide robots.txt and sitemap.xml, which must live at the domain
# root to be honored by crawlers.
SITE_ROOT_DIR = ROOT / "docs"
DOCS_DIR = SITE_ROOT_DIR / "spec"
TEMPLATE_PATH = ROOT / "templates" / "docs-page.html"
ASSETS_DIR = DOCS_DIR / "assets"

SITE_ORIGIN = "https://git-meta.com"
SITE_BASE = f"{SITE_ORIGIN}/spec"
# Base URL for raw markdown sources on GitHub. Each generated spec page
# embeds a "view markdown" link in the page header pointing at this prefix
# joined with the page's `source_rel` path, so readers can jump straight
# to the unrendered .md file.
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/git-meta/git-meta/main/spec"
# Path of the marketing landing page relative to docs/. Its mtime is used
# as the sitemap <lastmod> for the root URL so the sitemap stays accurate
# whenever the landing page is republished.
LANDING_PAGE_FILE = "index.html"

AI_USER_AGENTS = [
    "GPTBot",
    "OAI-SearchBot",
    "ChatGPT-User",
    "ClaudeBot",
    "Claude-Web",
    "anthropic-ai",
    "Google-Extended",
    "PerplexityBot",
    "CCBot",
    "Applebot-Extended",
    "Bytespider",
    "Meta-ExternalAgent",
    "cohere-ai",
]

PAGE_ORDER = [
    "README.md",
    "exchange-format/targets.md",
    "exchange-format/exchange.md",
    "exchange-format/materialization.md",
    "exchange-format/strings.md",
    "exchange-format/lists.md",
    "exchange-format/sets.md",
    "implementation/overview.md",
    "implementation/storage.md",
    "implementation/cli.md",
    "implementation/output.md",
    "implementation/pruning.md",
    "implementation/serialize-filters.md",
    "implementation/standard-keys.md",
    "implementation/remotes.md",
    "implementation/auto-sync.md",
    "implementation/workflow.md",
]

PAGE_GROUPS = {
    "": ["README.md"],
    "Exchange format": [
        "exchange-format/targets.md",
        "exchange-format/exchange.md",
        "exchange-format/materialization.md",
    ],
    "Value types": [
        "exchange-format/strings.md",
        "exchange-format/lists.md",
        "exchange-format/sets.md",
    ],
    "Implementation": [
        "implementation/overview.md",
        "implementation/storage.md",
        "implementation/cli.md",
        "implementation/output.md",
        "implementation/pruning.md",
        "implementation/serialize-filters.md",
    ],
    "Other Considerations": [
        "implementation/standard-keys.md",
        "implementation/remotes.md",
        "implementation/auto-sync.md",
        "implementation/workflow.md",
    ]
}

STYLE_CSS = """
:root {
  color-scheme: light dark;
  --max: 920px;
  --doc-max: 1264px;
  --aside-width: 260px;
  --sidebar-width: 260px;
  --toc-width: 240px;
}

:root,
:root[data-theme='dark'] {
  --bg: #0b1020;
  --panel: #121937;
  --panel-2: #0f1530;
  --text: #e6eaf2;
  --muted: #a3acc2;
  --link: #8ab4ff;
  --border: #27304f;
  --code: #0b1228;
  --accent: #7dd3fc;
  --callout: color-mix(in srgb, #12203b 72%, transparent);
  --callout-border: color-mix(in srgb, #335b94 70%, transparent);
  --button-bg: transparent;
  --button-hover: rgba(255,255,255,0.04);
  --button-active: rgba(255,255,255,0.06);
}

:root[data-theme='light'] {
  --bg: #f8fafc;
  --panel: #eef2ff;
  --panel-2: #e2e8f0;
  --text: #0f172a;
  --muted: #475569;
  --link: #1d4ed8;
  --border: #cbd5e1;
  --code: #f1f5f9;
  --accent: #0369a1;
  --callout: color-mix(in srgb, #eff6ff 78%, white);
  --callout-border: color-mix(in srgb, #60a5fa 58%, transparent);
  --button-bg: transparent;
  --button-hover: rgba(15,23,42,0.05);
  --button-active: rgba(15,23,42,0.08);
}

@media (prefers-color-scheme: light) {
  :root[data-theme='system'] {
    --bg: #f8fafc;
    --panel: #eef2ff;
    --panel-2: #e2e8f0;
    --text: #0f172a;
    --muted: #475569;
    --link: #1d4ed8;
    --border: #cbd5e1;
    --code: #f1f5f9;
    --accent: #0369a1;
    --callout: color-mix(in srgb, #eff6ff 78%, white);
    --callout-border: color-mix(in srgb, #60a5fa 58%, transparent);
    --button-bg: rgba(15,23,42,0.03);
    --button-hover: rgba(15,23,42,0.05);
    --button-active: rgba(15,23,42,0.07);
  }
}

* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; background: var(--bg); color: var(--text); }
body { font: 16px/1.6 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
body.sidebar-collapsed {
  --sidebar-width: 72px;
}
.layout { display: grid; grid-template-columns: var(--sidebar-width) minmax(0, 1fr); min-height: 100vh; }
body.has-toc .layout {
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr) var(--toc-width);
}
.sidebar {
  --sidebar-bg: #f7f3ec;
  --sidebar-text: #2a1f1a;
  --sidebar-muted: #6b5d54;
  --sidebar-border: #e3dccf;
  --sidebar-button-hover: rgba(42, 31, 26, 0.06);
  --sidebar-button-active: rgba(42, 31, 26, 0.10);
  background: var(--sidebar-bg);
  color: var(--sidebar-text);
  border-right: 1px solid var(--sidebar-border);
  padding: 28px 20px;
  position: sticky;
  top: 0;
  height: 100vh;
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.sidebar-main {
  min-height: 0;
  overflow-y: auto;
}
.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 1.2rem;
  font-weight: 800;
  color: var(--sidebar-text);
  margin-bottom: 14px;
  text-decoration: none;
}
.brand:hover { text-decoration: none; }
.brand-icon {
  width: 40px;
  height: 40px;
  border-radius: 8px;
  display: block;
  flex-shrink: 0;
}
.tagline { display: none; }
.sidebar-footer {
  margin-top: auto;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding-top: 12px;
  border-top: 1px solid var(--sidebar-border);
  opacity: 0.78;
}
.theme-control {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  padding: 2px;
  border: 1px solid var(--sidebar-border);
  border-radius: 999px;
  background: transparent;
}
.theme-icon-button,
.sidebar-toggle {
  appearance: none;
  border: 1px solid transparent;
  background: transparent;
  color: var(--sidebar-muted);
  border-radius: 999px;
  width: 28px;
  height: 28px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  font: inherit;
  font-size: 0.9rem;
}
.theme-icon-button:hover,
.sidebar-toggle:hover {
  background: var(--sidebar-button-hover);
  color: var(--sidebar-text);
}
.theme-icon-button.active {
  background: var(--sidebar-button-active);
  border-color: transparent;
  color: var(--sidebar-text);
}
.sidebar-toggle {
  border-color: var(--sidebar-border);
  background: transparent;
  font-size: 0.82rem;
}
.sidebar.collapsed {
  padding-left: 12px;
  padding-right: 12px;
}
.sidebar.collapsed .sidebar-main,
.sidebar.collapsed .theme-control {
  display: none;
}
.sidebar.collapsed .sidebar-footer {
  border-top: 0;
  padding-top: 0;
  justify-content: center;
}
.nav-group { margin-bottom: 22px; }
.nav-group h2 {
  font-size: 0.74rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--sidebar-muted);
  margin: 0 0 8px;
}
.nav a {
  display: block;
  padding: 6px 9px;
  border-radius: 8px;
  color: var(--sidebar-text);
  font-size: 0.92rem;
}
.nav a:hover {
  background: var(--sidebar-button-hover);
  text-decoration: none;
}
.nav a.active {
  background: var(--sidebar-button-active);
  color: var(--sidebar-text);
  font-weight: 600;
}
.content { padding: 32px 44px 60px; }
.page-header {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  margin-bottom: 24px;
}
.page-header-main { min-width: 0; flex: 1; }
.eyebrow { color: var(--muted); margin-bottom: 8px; }
/* Small chip-style link in the page header that points at the raw .md
   source for the current page, so readers can grab the unrendered
   markdown without hunting through the repo. Sits flush with the page
   title's top edge so it visually anchors to the heading. */
.page-source-link {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
  margin-top: 4px;
  padding: 4px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 0.8rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  color: var(--muted);
  background: var(--button-bg);
  text-decoration: none;
  white-space: nowrap;
}
.page-source-link:hover {
  color: var(--text);
  background: var(--button-hover);
  border-color: color-mix(in srgb, var(--link) 35%, var(--border));
}
.collapse-icon {
  display: inline-block;
  transform: rotate(0deg);
  transition: transform 0.15s ease;
}
.sidebar.collapsed .collapse-icon {
  transform: rotate(180deg);
}
.doc-content {
  max-width: var(--doc-max);
}
.doc-content h1, .doc-content h2, .doc-content h3, .doc-content h4 {
  line-height: 1.25;
  margin-top: 1.7em;
  margin-bottom: 0.5em;
}
.doc-content h1:first-child, .doc-content h2:first-child { margin-top: 0; }
.doc-content p, .doc-content ul, .doc-content ol, .doc-content pre, .doc-content blockquote, .doc-content .callout {
  margin: 0 0 1rem;
}
.doc-content ul, .doc-content ol { padding-left: 1.5rem; }
.doc-content li + li { margin-top: 0.35rem; }
.doc-content code {
  background: color-mix(in srgb, var(--text) 8%, transparent);
  padding: 0.12rem 0.35rem;
  border-radius: 6px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.92em;
}
.doc-content pre {
  background: var(--code);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 14px 16px;
  overflow-x: auto;
}
.doc-content pre code { background: transparent; padding: 0; }
.doc-content blockquote,
.callout {
  border-left: 3px solid var(--callout-border);
  background: var(--callout);
  padding: 10px 12px;
  border-radius: 10px;
  font-size: 0.95rem;
  line-height: 1.5;
}
.callout-title {
  font-weight: 700;
  margin-bottom: 0.3rem;
  font-size: 0.95rem;
}
.callout p {
  margin-bottom: 0.7rem;
}
.callout-youtube-link {
  display: block;
  position: relative;
  border-radius: 8px;
  overflow: hidden;
  background: #000;
}
.callout-youtube-thumb {
  aspect-ratio: 16 / 9;
  width: 100%;
  display: block;
  object-fit: cover;
}
.callout-youtube-play {
  position: absolute;
  inset: 50% auto auto 50%;
  transform: translate(-50%, -50%);
  width: 52px;
  height: 38px;
  border-radius: 10px;
  background: rgba(15, 23, 42, 0.78);
  color: white;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 1.1rem;
  box-shadow: 0 2px 12px rgba(0,0,0,0.22);
}
.callout-youtube-caption {
  margin-top: 0.7rem;
  color: var(--muted);
  font-size: 0.9em;
}
.doc-content hr { border: 0; border-top: 1px solid var(--border); margin: 2rem 0; }
.doc-content table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; }
.doc-content th, .doc-content td { border: 1px solid var(--border); padding: 0.6rem 0.7rem; text-align: left; }
.doc-content th { background: color-mix(in srgb, var(--text) 4%, transparent); }
/* ——— Aside callouts that float in the content column ———
   `[!YOUTUBE]` (and any future "sidebar"-feeling callout) becomes a
   `.callout-aside`. It floats to the right of the prose so adjacent
   paragraphs wrap around it — the same placement it had before the
   right-hand TOC was introduced. The float lives *inside* the content
   column, so it never collides with the TOC sidebar in the third
   grid track. */
/* ——— Key card component ———
   Triggered by a fenced block with the `key` info-string in the
   markdown source:

       ```key agent:provider
       type: string
       meaning: service or runtime provider
       examples:
         - openai
         - anthropic
       ```

   Renders as a structured card with a name pill, a type badge, the
   meaning, and either an examples chip row or a format string. */
.key-card {
  /* `--key-color` is overridden per type below; defaults to the
     neutral border so untyped cards still render cleanly. */
  --key-color: var(--border);
  position: relative;
  border: 1px solid var(--border);
  border-top: 3px solid var(--key-color);
  background: color-mix(in srgb, var(--text) 3%, transparent);
  border-radius: 8px;
  padding: 0 14px 12px;
  margin: 0 0 0.85rem;
  overflow: hidden;
}
.key-card + .key-card { margin-top: 0.85rem; }
.key-card.is-string { --key-color: #38bdf8; }
.key-card.is-list   { --key-color: #34d399; }
.key-card.is-set    { --key-color: #c084fc; }
.key-card-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 0 -14px 8px;
  padding: 6px 14px;
  background: color-mix(in srgb, var(--key-color) 12%, transparent);
  border-bottom: 1px solid color-mix(in srgb, var(--key-color) 30%, var(--border));
}
/* Selector specificity matches `.doc-content h3` so the global heading
   margin (1.7em top / 0.5em bottom) doesn't push the title down inside
   the header band. line-height is overridden too because the global
   1.25 still leaves room for visible chrome above/below the glyphs. */
.key-card .key-card-name {
  margin: 0;
  font-size: 1rem;
  font-weight: 700;
  line-height: 1.1;
}
.key-card-name code {
  background: transparent;
  padding: 0;
  font-size: inherit;
}
.key-card-type {
  margin-left: auto;
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-weight: 700;
  color: color-mix(in srgb, var(--key-color) 70%, var(--text));
  background: var(--bg);
  border: 1px solid color-mix(in srgb, var(--key-color) 45%, var(--border));
  border-radius: 999px;
  padding: 2px 9px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
.key-card-meaning {
  margin: 0 0 8px;
  color: var(--text);
}
.key-card-meaning:last-child { margin-bottom: 0; }
.key-card-section { margin-top: 10px; }
.key-card-section:first-of-type { margin-top: 0; }
.key-card-section p { margin: 0; }
.key-card-label {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-weight: 600;
  color: var(--muted);
  margin-bottom: 5px;
}
.key-card-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}
.key-card-chip {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 1px 7px;
  font-size: 0.85rem;
}
/* Footer band that mirrors the header band (full-bleed, tinted with
   the per-type --key-color) and lists the target types this key may
   be attached to. Rendered only when the source `key` block declares
   `targets:`. */
.key-card-footer {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin: 12px -14px -12px;
  padding: 6px 14px;
  background: color-mix(in srgb, var(--key-color) 10%, transparent);
  border-top: 1px solid color-mix(in srgb, var(--key-color) 25%, var(--border));
  color: var(--muted);
}
.key-card-footer-label {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-weight: 600;
}
.key-card-targets {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 5px;
}
.key-card-target {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.75rem;
  color: color-mix(in srgb, var(--key-color) 70%, var(--text));
  background: var(--bg);
  border: 1px solid color-mix(in srgb, var(--key-color) 35%, var(--border));
  border-radius: 999px;
  padding: 1px 8px;
}

.callout-aside {
  float: right;
  width: var(--aside-width);
  max-width: 100%;
  margin: 0.25rem 0 1rem 1.25rem;
  clear: right;
  background: color-mix(in srgb, var(--text) 4%, transparent);
  border: 1px solid var(--border);
  padding: 14px;
  border-radius: 12px;
}
.callout-aside .callout-youtube-link { border-radius: 8px; }
.callout-aside .callout-youtube-caption {
  margin-top: 0.6rem;
  margin-bottom: 0;
}
.callout-aside .callout-youtube-caption p:last-child { margin-bottom: 0; }
@media (max-width: 720px) {
  /* Single-column reading width — let the aside take the full content
     measure inline rather than squeezing it next to the prose. */
  .callout-aside {
    float: none;
    width: auto;
    margin: 0 0 1rem;
  }
}

/* ——— Right-hand "On this page" TOC sidebar ———
   The TOC lives in the third grid column when the page has any h2/h3
   headings (body.has-toc). It's sticky, scrollable independently of the
   page, and quietly hides on narrower viewports so the content column
   gets the full width. */
.toc-aside {
  position: sticky;
  top: 0;
  align-self: start;
  max-height: 100vh;
  overflow-y: auto;
  padding: 32px 24px 60px 8px;
  border-left: 1px solid var(--border);
  font-size: 0.9rem;
  line-height: 1.45;
}
.toc-title {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  font-weight: 600;
  margin-bottom: 10px;
}
.toc-list, .toc-sub {
  list-style: none;
  padding: 0;
  margin: 0;
}
.toc-list > li + li { margin-top: 4px; }
.toc-sub {
  margin: 4px 0 6px 12px;
  padding-left: 10px;
  border-left: 1px solid var(--border);
}
.toc-sub > li + li { margin-top: 2px; }
.toc-aside a {
  display: block;
  padding: 4px 8px;
  border-radius: 6px;
  color: var(--muted);
  text-decoration: none;
  border-left: 2px solid transparent;
  transition: color 0.12s ease, background 0.12s ease, border-color 0.12s ease;
}
.toc-aside a:hover {
  color: var(--text);
  background: color-mix(in srgb, var(--text) 5%, transparent);
  text-decoration: none;
}
.toc-aside a.active {
  color: var(--text);
  border-left-color: var(--accent);
  background: color-mix(in srgb, var(--accent) 8%, transparent);
}

@media (max-width: 1180px) {
  body.has-toc .layout {
    grid-template-columns: var(--sidebar-width) minmax(0, 1fr);
  }
  .toc-aside { display: none; }
}
@media (max-width: 960px) {
  body.sidebar-collapsed {
    --sidebar-width: 1fr;
  }
  .layout,
  body.has-toc .layout { grid-template-columns: 1fr; }
  .sidebar { display: none; }
  .content { padding: 24px 18px 48px; }
}
"""

@dataclass
class Page:
    source_rel: str
    source_path: Path
    output_rel: str
    output_path: Path
    title: str


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text or "section"


def youtube_video_id(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None

    patterns = [
        r"(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([A-Za-z0-9_-]{11})",
        r"(?:https?://)?(?:www\.)?youtu\.be/([A-Za-z0-9_-]{11})",
        r"(?:https?://)?(?:www\.)?youtube\.com/embed/([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            return match.group(1)
    return None


def inline_format(text: str, page_map: dict[str, str], current_page: Page) -> str:
    text = html.escape(text)
    text = re.sub(r"`([^`]+)`", lambda m: f"<code>{m.group(1)}</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", text)

    def repl_link(match: re.Match[str]) -> str:
        label = match.group(1)
        url = match.group(2)
        if url.endswith(".md") or ".md#" in url:
            if "#" in url:
                path_part, anchor = url.split("#", 1)
                suffix = f"#{slugify(anchor)}"
            else:
                path_part, suffix = url, ""
            resolved = str(((SPEC_DIR / current_page.source_rel).parent / path_part).resolve().relative_to(SPEC_DIR.resolve()))
            target_output = page_map.get(resolved, path_part[:-3] + ".html")
            url = str(Path(os.path.relpath(DOCS_DIR / target_output, current_page.output_path.parent))).replace("\\", "/") + suffix
        return f'<a href="{html.escape(url)}">{label}</a>'

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", repl_link, text)
    return text


def parse_key_card_body(body_lines: list[str]) -> dict[str, str | list[str]]:
    """Parse a YAML-ish key-card body into a field dict.

    Top-level lines like ``type: string`` become scalar entries.
    A line ending in ``:`` (e.g. ``examples:``) opens a list whose
    items are subsequent indented ``- value`` lines. Surrounding
    quotes around scalar values are stripped for convenience.
    """
    result: dict[str, str | list[str]] = {}
    current_list: list[str] | None = None
    current_field: str | None = None
    for raw in body_lines:
        line = raw.rstrip()
        if not line.strip():
            continue
        # Indented list item belonging to the most recent field.
        if current_list is not None and re.match(r"^\s+[-*]\s+", line):
            item = re.sub(r"^\s+[-*]\s+", "", line).strip()
            if item.startswith(("'", '"')) and item.endswith(item[0]) and len(item) > 1:
                item = item[1:-1]
            current_list.append(item)
            continue
        # New top-level field.
        m = re.match(r"^([A-Za-z][\w-]*)\s*:\s*(.*)$", line)
        if m:
            current_field = m.group(1)
            value = m.group(2).strip()
            if value:
                if value.startswith(("'", '"')) and value.endswith(value[0]) and len(value) > 1:
                    value = value[1:-1]
                result[current_field] = value
                current_list = None
            else:
                current_list = []
                result[current_field] = current_list
            continue
        # Continuation line for a scalar field.
        if current_field and isinstance(result.get(current_field), str):
            result[current_field] = (str(result[current_field]) + " " + line.strip()).strip()
    return result


def render_key_card(
    name: str,
    body_lines: list[str],
    page_map: dict[str, str],
    current_page: "Page",
) -> tuple[str, str, str]:
    """Render a ``key <name>`` fenced block as a structured card.

    Returns ``(html, anchor, heading_html)``. The anchor and heading
    HTML are pushed into the page's heading list by the caller so the
    right-hand TOC can link into the card.
    """
    data = parse_key_card_body(body_lines)
    anchor = slugify(name)
    name_html = f"<code>{html.escape(name)}</code>"

    type_value = data.get("type")
    # `is-{type}` on the card root drives the per-type color stripe,
    # header tint, and pill accent (see `--key-color` in the CSS).
    type_class = (
        f" is-{html.escape(type_value)}"
        if isinstance(type_value, str) and type_value
        else ""
    )
    parts: list[str] = [f'<section class="key-card{type_class}" id="{html.escape(anchor)}">']
    header: list[str] = [f'<h3 class="key-card-name">{name_html}</h3>']
    if isinstance(type_value, str) and type_value:
        header.append(f'<span class="key-card-type">{html.escape(type_value)}</span>')
    parts.append(f'<div class="key-card-header">{"".join(header)}</div>')

    meaning = data.get("meaning")
    if isinstance(meaning, str) and meaning:
        parts.append(
            f'<p class="key-card-meaning">{inline_format(meaning, page_map, current_page)}</p>'
        )

    def section(label: str, body: str) -> str:
        return (
            '<div class="key-card-section">'
            f'<div class="key-card-label">{html.escape(label)}</div>'
            f'{body}'
            '</div>'
        )

    fmt = data.get("format")
    if isinstance(fmt, str) and fmt:
        parts.append(section("Format", f'<p>{inline_format(fmt, page_map, current_page)}</p>'))

    examples = data.get("examples")
    if isinstance(examples, list) and examples:
        chips = "".join(
            f'<code class="key-card-chip">{html.escape(item)}</code>' for item in examples
        )
        parts.append(section("Examples", f'<div class="key-card-chips">{chips}</div>'))

    # `targets:` constrains the key to specific target types. Accepts a
    # bare scalar (`targets: commit`), a comma-separated scalar
    # (`targets: commit, change-id`), or a YAML-style nested list. All
    # three normalise to a list of trimmed target names rendered as
    # chips in a footer band coloured with the card's per-type accent.
    targets_raw = data.get("targets")
    target_list: list[str] = []
    if isinstance(targets_raw, list):
        target_list = [t.strip() for t in targets_raw if t.strip()]
    elif isinstance(targets_raw, str) and targets_raw.strip():
        target_list = [t.strip() for t in targets_raw.split(",") if t.strip()]

    if target_list:
        chips = "".join(
            f'<code class="key-card-target">{html.escape(t)}</code>' for t in target_list
        )
        parts.append(
            '<footer class="key-card-footer">'
            '<span class="key-card-footer-label">Attach to</span>'
            f'<span class="key-card-targets">{chips}</span>'
            '</footer>'
        )

    parts.append("</section>")
    return "".join(parts), anchor, name_html


def split_table_row(line: str) -> list[str]:
    """Split a markdown pipe-table row into trimmed cell strings.

    Respects inline-code spans (``` `…` ```) so a literal ``|`` inside
    backticks is preserved as cell content instead of being treated as a
    column separator. A single optional leading and trailing ``|`` is
    stripped so both ``| a | b |`` and ``a | b`` parse identically.
    Backslash-escaped pipes (``\\|``) are also preserved as literal
    ``|`` characters within a cell.
    """
    s = line.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|") and not s.endswith("\\|"):
        s = s[:-1]
    cells: list[str] = []
    buf: list[str] = []
    in_code = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "`":
            in_code = not in_code
            buf.append(ch)
        elif ch == "\\" and i + 1 < len(s) and s[i + 1] == "|":
            buf.append("|")
            i += 1
        elif ch == "|" and not in_code:
            cells.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    cells.append("".join(buf).strip())
    return cells


def is_table_separator(line: str) -> bool:
    """True if ``line`` is a markdown pipe-table header/body separator.

    A separator contains at least one ``|`` plus cells made entirely of
    dashes with optional leading/trailing ``:`` for alignment hints
    (``:---``, ``---:``, ``:---:``). Requiring a ``|`` prevents the
    bare-``---`` horizontal-rule line from being misclassified.
    """
    s = line.strip()
    if "|" not in s:
        return False
    cells = split_table_row(s)
    if not cells:
        return False
    return all(re.match(r"^:?-+:?$", c) for c in cells)


def table_alignments(separator_line: str) -> list[str]:
    """Return per-column CSS ``text-align`` values from a separator row.

    Empty string means "no explicit alignment" (let the browser default
    apply). ``:---`` -> left, ``---:`` -> right, ``:---:`` -> center.
    """
    aligns: list[str] = []
    for cell in split_table_row(separator_line):
        left = cell.startswith(":")
        right = cell.endswith(":")
        if left and right:
            aligns.append("center")
        elif right:
            aligns.append("right")
        elif left:
            aligns.append("left")
        else:
            aligns.append("")
    return aligns


def markdown_to_html(
    markdown_text: str, page_map: dict[str, str], current_page: Page
) -> tuple[str, str, bool, list[tuple[int, str, str]]]:
    """Render markdown to HTML and collect heading metadata for the TOC.

    Returns a 4-tuple of ``(html, title, has_callout, headings)`` where
    ``headings`` is a list of ``(level, anchor, text_html)`` for every
    heading at level 2 or deeper. ``text_html`` is the inline-formatted
    HTML for the heading (e.g. ``<code>agent:provider</code>``), already
    safe to embed verbatim in the TOC. The page-title h1 is consumed
    into ``title`` and excluded so the right-hand TOC doesn't repeat it.
    """
    lines = markdown_text.splitlines()
    out: list[str] = []
    in_code = False
    code_lines: list[str] = []
    # List items carry the source indent (in raw whitespace columns) so
    # `render_list` below can fold sibling/child rows into nested
    # `<ul>`/`<ol>` structures rather than emitting one flat list.
    ul_items: list[tuple[int, str]] = []
    ol_items: list[tuple[int, str]] = []
    paragraph: list[str] = []
    title = "Untitled"
    has_callout = False
    headings: list[tuple[int, str, str]] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            out.append(f"<p>{inline_format(' '.join(paragraph).strip(), page_map, current_page)}</p>")
            paragraph = []

    def render_list(items: list[tuple[int, str]], tag: str) -> str:
        """Render a flat (indent, content) list as nested ``<ul>``/``<ol>``.

        Items with an indent strictly greater than the previous one are
        wrapped in a child list that lives inside the previous ``<li>``,
        mirroring the way Markdown indents continuation bullets. Returns
        a single concatenated HTML string with no leading/trailing
        whitespace.
        """
        parts: list[str] = []
        open_indents: list[int] = []
        pending_li_close = False
        for indent, content in items:
            while open_indents and open_indents[-1] > indent:
                if pending_li_close:
                    parts.append("</li>")
                    pending_li_close = False
                parts.append(f"</{tag}>")
                open_indents.pop()
                # The parent <li> at the now-current indent is still
                # open and will need closing before the next sibling.
                pending_li_close = True

            if not open_indents or open_indents[-1] < indent:
                parts.append(f"<{tag}>")
                open_indents.append(indent)
                pending_li_close = False
            elif pending_li_close:
                parts.append("</li>")

            parts.append(f"<li>{content}")
            pending_li_close = True

        while open_indents:
            if pending_li_close:
                parts.append("</li>")
                pending_li_close = False
            parts.append(f"</{tag}>")
            open_indents.pop()
            pending_li_close = bool(open_indents)
        return "".join(parts)

    def flush_lists() -> None:
        nonlocal ul_items, ol_items
        if ul_items:
            out.append(render_list(ul_items, "ul"))
            ul_items = []
        if ol_items:
            out.append(render_list(ol_items, "ol"))
            ol_items = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if in_code:
            if stripped.startswith("```"):
                out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code = False
            else:
                code_lines.append(line)
            i += 1
            continue

        if stripped.startswith("```"):
            flush_paragraph()
            flush_lists()
            info = stripped[3:].strip()
            # `key` is a custom block: `\`\`\`key <name>` opens a
            # structured "key card" component (see `render_key_card`).
            # Anything else is a normal code block.
            if info.split(" ", 1)[0] == "key":
                name = info[3:].strip()
                i += 1
                body_lines: list[str] = []
                while i < len(lines) and not lines[i].lstrip().startswith("```"):
                    body_lines.append(lines[i])
                    i += 1
                if i < len(lines):
                    i += 1
                # Cards are intentionally NOT added to the TOC: the
                # parent section heading already represents the group,
                # and listing every key would crowd the rail. Direct
                # links into a card still work via the card's anchor id.
                card_html, _anchor, _heading_html = render_key_card(
                    name, body_lines, page_map, current_page
                )
                out.append(card_html)
                continue
            in_code = True
            i += 1
            continue

        if not stripped:
            flush_paragraph()
            flush_lists()
            i += 1
            continue

        if stripped == "---":
            flush_paragraph()
            flush_lists()
            out.append("<hr>")
            i += 1
            continue

        m = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if m:
            flush_paragraph()
            flush_lists()
            level = len(m.group(1))
            text = m.group(2).strip()
            if level == 1 and title == "Untitled":
                title = text
                i += 1
                continue
            anchor = slugify(text)
            text_html = inline_format(text, page_map, current_page)
            out.append(f'<h{level} id="{anchor}">{text_html}</h{level}>')
            # Store the inline-formatted HTML (not the raw markdown) so
            # the TOC renders code spans, italics, etc. consistently with
            # the heading itself.
            headings.append((level, anchor, text_html))
            i += 1
            continue

        if stripped.startswith(">"):
            flush_paragraph()
            flush_lists()
            quote_lines = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                quote_lines.append(lines[i].strip()[1:].lstrip())
                i += 1
            if quote_lines and re.match(r"\[![A-Z]+\]", quote_lines[0]):
                kind_match = re.match(r"\[!([A-Z]+)\]", quote_lines[0])
                kind_key = kind_match.group(1)
                kind = kind_key.title()
                body = quote_lines[1:] if len(quote_lines) > 1 else []
                has_callout = True
                if kind_key == "YOUTUBE":
                    video_id = youtube_video_id(body[0] if body else "")
                    caption_lines = body[1:] if len(body) > 1 else []
                    if video_id:
                        caption_html = "".join(f"<p>{inline_format(x, page_map, current_page)}</p>" for x in caption_lines if x)
                        if caption_html:
                            caption_html = f'<div class="callout-youtube-caption">{caption_html}</div>'
                        body_html = (
                            f'<a class="callout-youtube-link" href="https://www.youtube.com/watch?v={html.escape(video_id)}" target="_blank" rel="noopener noreferrer">'
                            f'<img class="callout-youtube-thumb" src="https://i.ytimg.com/vi/{html.escape(video_id)}/hqdefault.jpg" alt="YouTube video thumbnail" loading="lazy">'
                            '<span class="callout-youtube-play" aria-hidden="true">▶</span>'
                            '</a>'
                            f'{caption_html}'
                        )
                    else:
                        body_html = "".join(f"<p>{inline_format(x, page_map, current_page)}</p>" for x in body if x)
                    # YouTube callouts float into the right margin of the
                    # content column on wide viewports (see CSS
                    # `.callout-aside`). On narrow viewports they fall
                    # back to a full-width inline card.
                    out.append(f'<div class="callout callout-aside">{body_html}</div>')
                else:
                    body_html = "".join(f"<p>{inline_format(x, page_map, current_page)}</p>" for x in body if x)
                    # Inline callouts (NOTE, WARNING, …) stay anchored to
                    # the paragraph they follow; they read better next to
                    # the prose than floated into the margin.
                    out.append(
                        f'<div class="callout callout-{kind_key.lower()}">'
                        f'<div class="callout-title">{kind}</div>{body_html}</div>'
                    )
            else:
                body = " ".join(quote_lines)
                out.append(f"<blockquote><p>{inline_format(body, page_map, current_page)}</p></blockquote>")
            continue

        if (
            stripped.startswith("|")
            and i + 1 < len(lines)
            and is_table_separator(lines[i + 1])
        ):
            flush_paragraph()
            flush_lists()
            header_cells = split_table_row(line)
            aligns = table_alignments(lines[i + 1])
            j = i + 2
            body_rows: list[list[str]] = []
            while j < len(lines) and lines[j].strip().startswith("|"):
                body_rows.append(split_table_row(lines[j]))
                j += 1

            def _style(idx: int, aligns: list[str] = aligns) -> str:
                if idx < len(aligns) and aligns[idx]:
                    return f' style="text-align:{aligns[idx]}"'
                return ""

            parts: list[str] = ["<table><thead><tr>"]
            for idx, cell in enumerate(header_cells):
                parts.append(
                    f"<th{_style(idx)}>{inline_format(cell, page_map, current_page)}</th>"
                )
            parts.append("</tr></thead>")
            if body_rows:
                parts.append("<tbody>")
                for row in body_rows:
                    parts.append("<tr>")
                    for idx in range(len(header_cells)):
                        cell = row[idx] if idx < len(row) else ""
                        parts.append(
                            f"<td{_style(idx)}>{inline_format(cell, page_map, current_page)}</td>"
                        )
                    parts.append("</tr>")
                parts.append("</tbody>")
            parts.append("</table>")
            out.append("".join(parts))
            i = j
            continue

        m_ul = re.match(r"^(\s*)[-*]\s+(.*)$", line.rstrip())
        if m_ul:
            flush_paragraph()
            if ol_items:
                flush_lists()
            indent = len(m_ul.group(1).expandtabs(4))
            ul_items.append((indent, inline_format(m_ul.group(2), page_map, current_page)))
            i += 1
            continue

        m_ol = re.match(r"^(\s*)\d+\.\s+(.*)$", line.rstrip())
        if m_ol:
            flush_paragraph()
            if ul_items:
                flush_lists()
            indent = len(m_ol.group(1).expandtabs(4))
            ol_items.append((indent, inline_format(m_ol.group(2), page_map, current_page)))
            i += 1
            continue

        paragraph.append(stripped)
        i += 1

    flush_paragraph()
    flush_lists()
    if in_code:
        out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
    return "\n".join(out), title, has_callout, headings


def build_toc(headings: list[tuple[int, str, str]]) -> str:
    """Render a nested ``<aside>`` TOC for the right-hand sidebar.

    Only level-2 (``##``) and level-3 (``###``) headings are surfaced;
    deeper levels rarely add navigational value and would clutter the
    rail. Returns an empty string when there's nothing to show, which
    lets the layout collapse the third grid column for short pages.

    H3s are nested under the most recent H2. Orphan H3s (any H3 that
    appears before the page's first H2) are promoted to top-level
    entries so they're still navigable.

    Heading text is treated as already-safe HTML (see
    ``markdown_to_html``) so code spans in the markdown source (e.g.
    `` `agent:provider` ``) show up as ``<code>agent:provider</code>``
    in the rail rather than as literal backticks.
    """
    items = [(level, anchor, text) for level, anchor, text in headings if level in (2, 3)]
    if not items:
        return ""

    # Build a simple tree: list of (anchor, text_html, [children]) where
    # each child is (anchor, text_html). Render in one pass below.
    tree: list[tuple[str, str, list[tuple[str, str]]]] = []
    for level, anchor, text_html in items:
        if level == 2 or not tree:
            tree.append((anchor, text_html, []))
        else:
            tree[-1][2].append((anchor, text_html))

    parts: list[str] = []
    for anchor, text_html, children in tree:
        link = f'<a href="#{html.escape(anchor)}">{text_html}</a>'
        if children:
            sub = "".join(
                f'<li class="toc-h3"><a href="#{html.escape(c_anchor)}">{c_text_html}</a></li>'
                for c_anchor, c_text_html in children
            )
            parts.append(f'<li class="toc-h2">{link}<ul class="toc-sub">{sub}</ul></li>')
        else:
            parts.append(f'<li class="toc-h2">{link}</li>')

    return (
        '<aside class="toc-aside" aria-label="On this page">'
        '<div class="toc-title">On this page</div>'
        f'<ul class="toc-list">{"".join(parts)}</ul>'
        "</aside>"
    )


def read_title(path: Path) -> str:
    for line in path.read_text().splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return path.stem


def output_rel_for(source_rel: str) -> str:
    if source_rel == "README.md":
        return "index.html"
    return source_rel.replace(".md", ".html")


def build_pages() -> list[Page]:
    pages = []
    for source_rel in PAGE_ORDER:
        source_path = SPEC_DIR / source_rel
        output_rel = output_rel_for(source_rel)
        output_path = DOCS_DIR / output_rel
        pages.append(Page(source_rel, source_path, output_rel, output_path, read_title(source_path)))
    return pages


def root_prefix(page: Page) -> str:
    depth = len(Path(page.output_rel).parents) - 1
    return "../" * depth


def build_nav(pages: list[Page], current_page: Page) -> str:
    page_lookup = {page.source_rel: page for page in pages}
    groups = []
    for group_name, members in PAGE_GROUPS.items():
        links = []
        for member in members:
            page = page_lookup[member]
            href = os.path.relpath(page.output_path, current_page.output_path.parent).replace("\\", "/")
            active = ' class="active"' if page.output_rel == current_page.output_rel else ""
            label = "Overview" if member == "README.md" else page.title
            links.append(f'<a{active} href="{href}">{html.escape(label)}</a>')
        heading = f'<h2>{html.escape(group_name)}</h2>' if group_name else ''
        groups.append(f'<div class="nav-group">{heading}{"".join(links)}</div>')
    return "".join(groups)


def page_url(page: Page) -> str:
    """Canonical absolute URL for a generated page on the spec sub-site."""
    return f"{SITE_BASE}/{page.output_rel}"


def page_lastmod(page: Page) -> str:
    """ISO-8601 date for a page's last modification, derived from its spec source."""
    try:
        mtime = page.source_path.stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc).date().isoformat()
    except OSError:
        return date.today().isoformat()


def write_robots_txt() -> None:
    """Write the site-wide robots.txt advertising open AI / search use.

    Crawlers only honor `/robots.txt` at the domain root, so this writes to
    `docs/robots.txt`. It covers the marketing landing page and the spec
    sub-site under the same allow-everything policy.
    """
    lines: list[str] = [
        "# robots.txt for git-meta.com",
        "# The git-meta landing page and specification are public and intended",
        "# for broad reuse, including by AI systems that index, search, or",
        "# train on documentation.",
        "",
        "User-agent: *",
        "Allow: /",
        "",
        "# Explicit rules for AI crawlers (RFC 9309)",
    ]
    for agent in AI_USER_AGENTS:
        lines.append(f"User-agent: {agent}")
        lines.append("Allow: /")
        lines.append("")

    lines.extend([
        "# Content Signals (https://contentsignals.org/)",
        "# search:   allow appearing in search results",
        "# ai-input: allow use as grounding input for AI answers",
        "# ai-train: allow use as training data for AI models",
        "Content-Signal: search=yes, ai-input=yes, ai-train=yes",
        "",
        f"Sitemap: {SITE_ORIGIN}/sitemap.xml",
        "",
    ])

    (SITE_ROOT_DIR / "robots.txt").write_text("\n".join(lines))


def landing_page_lastmod() -> str:
    """ISO-8601 date for the marketing landing page's last modification.

    Falls back to today if `docs/index.html` is missing for any reason
    (e.g. a fresh checkout where the landing page hasn't been added yet).
    """
    try:
        mtime = (SITE_ROOT_DIR / LANDING_PAGE_FILE).stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc).date().isoformat()
    except OSError:
        return date.today().isoformat()


def write_sitemap_xml(pages: list[Page]) -> None:
    """Write the site-wide sitemap.xml at the domain root.

    Includes the marketing landing page (`https://git-meta.com/`) followed by
    every generated spec page. Each spec entry uses its source markdown's
    mtime as <lastmod>; the landing page uses `docs/index.html`'s mtime.
    """
    entries: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
        "  <url>",
        f"    <loc>{html.escape(SITE_ORIGIN)}/</loc>",
        f"    <lastmod>{landing_page_lastmod()}</lastmod>",
        "  </url>",
    ]
    for page in pages:
        loc = page_url(page)
        if page.output_rel == "index.html":
            loc = f"{SITE_BASE}/"
        entries.append("  <url>")
        entries.append(f"    <loc>{html.escape(loc)}</loc>")
        entries.append(f"    <lastmod>{page_lastmod(page)}</lastmod>")
        entries.append("  </url>")
    entries.append("</urlset>")
    entries.append("")

    (SITE_ROOT_DIR / "sitemap.xml").write_text("\n".join(entries))


def generate_docs() -> None:
    pages = build_pages()
    page_map = {page.source_rel: page.output_rel for page in pages}
    template = TEMPLATE_PATH.read_text()

    # docs/spec/ is fully owned by this generator: wipe and rewrite it on
    # every run. Anything outside (the marketing landing page at docs/, the
    # hand-curated docs/other/ assets, docs/CNAME) is intentionally untouched.
    if DOCS_DIR.exists():
        shutil.rmtree(DOCS_DIR)
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    (ASSETS_DIR / "style.css").write_text(STYLE_CSS.strip() + "\n")

    for page in pages:
        markdown_text = page.source_path.read_text()
        content, detected_title, has_callout, headings = markdown_to_html(
            markdown_text, page_map, page
        )
        title = detected_title or page.title
        nav = build_nav(pages, page)
        root = root_prefix(page)
        toc = build_toc(headings)
        # `has-aside` lets the content column reserve right-side margin
        # so floating `.callout-aside` blocks don't crowd the prose. The
        # leading space keeps it from concatenating onto `doc-content`.
        content_class = " has-aside" if has_callout else ""
        body_class = "has-toc" if toc else ""
        markdown_url = f"{GITHUB_RAW_BASE}/{page.source_rel}"
        rendered = (
            template.replace("{{title}}", html.escape(title))
            .replace("{{nav}}", nav)
            .replace("{{content}}", content)
            .replace("{{content_class}}", content_class)
            .replace("{{toc}}", toc)
            .replace("{{body_class}}", body_class)
            .replace("{{markdown_url}}", html.escape(markdown_url))
            .replace("{{root}}", root)
        )
        page.output_path.parent.mkdir(parents=True, exist_ok=True)
        page.output_path.write_text(rendered)

    write_robots_txt()
    write_sitemap_xml(pages)

    print(f"Generated {len(pages)} documentation pages in {DOCS_DIR}")


def watch_signature() -> tuple[tuple[str, int], ...]:
    return tuple(
        sorted(
            (str(path.relative_to(ROOT)), path.stat().st_mtime_ns)
            for path in SPEC_DIR.rglob("*.md")
            if path.is_file()
        )
    )


def watch_docs(interval: float) -> None:
    generate_docs()
    last_signature = watch_signature()
    print(f"Watching {SPEC_DIR} for changes every {interval:.1f}s")

    while True:
        time.sleep(interval)
        current_signature = watch_signature()
        if current_signature == last_signature:
            continue
        last_signature = current_signature
        try:
            generate_docs()
        except Exception as exc:
            print(f"Doc generation failed: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate git-meta docs from spec markdown")
    parser.add_argument("-w", "--watch", action="store_true", help="watch spec/ for changes and regenerate docs")
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="polling interval in seconds for --watch mode (default: 0.5)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.watch:
        watch_docs(args.interval)
    else:
        generate_docs()


if __name__ == "__main__":
    main()

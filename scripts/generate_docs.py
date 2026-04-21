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
    "implementation/standard-keys.md",
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
    ],
    "Other Considerations": [
        "implementation/standard-keys.md",
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
  margin-bottom: 24px;
}
.page-header-main { min-width: 0; }
.eyebrow { color: var(--muted); margin-bottom: 8px; }
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
@media (min-width: 1100px) {
  .doc-content.has-aside {
    padding-right: calc(var(--aside-width) + 24px);
  }
  .callout.callout-aside {
    clear: right;
    float: right;
    width: var(--aside-width);
    margin: 0 calc(-1 * (var(--aside-width) + 24px)) 1rem 24px;
  }
}
@media (max-width: 960px) {
  body.sidebar-collapsed {
    --sidebar-width: 1fr;
  }
  .layout { grid-template-columns: 1fr; }
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


def markdown_to_html(markdown_text: str, page_map: dict[str, str], current_page: Page) -> tuple[str, str, bool]:
    lines = markdown_text.splitlines()
    out: list[str] = []
    in_code = False
    code_lines: list[str] = []
    ul_items: list[str] = []
    ol_items: list[str] = []
    paragraph: list[str] = []
    title = "Untitled"
    has_callout = False

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            out.append(f"<p>{inline_format(' '.join(paragraph).strip(), page_map, current_page)}</p>")
            paragraph = []

    def flush_lists() -> None:
        nonlocal ul_items, ol_items
        if ul_items:
            out.append("<ul>" + "".join(ul_items) + "</ul>")
            ul_items = []
        if ol_items:
            out.append("<ol>" + "".join(ol_items) + "</ol>")
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
            out.append(f'<h{level} id="{anchor}">{inline_format(text, page_map, current_page)}</h{level}>')
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
                    out.append(f'<div class="callout callout-aside">{body_html}</div>')
                else:
                    body_html = "".join(f"<p>{inline_format(x, page_map, current_page)}</p>" for x in body if x)
                    out.append(f'<div class="callout callout-aside"><div class="callout-title">{kind}</div>{body_html}</div>')
            else:
                body = " ".join(quote_lines)
                out.append(f"<blockquote><p>{inline_format(body, page_map, current_page)}</p></blockquote>")
            continue

        if re.match(r"^[-*]\s+", stripped):
            flush_paragraph()
            if ol_items:
                flush_lists()
            ul_items.append(f"<li>{inline_format(re.sub(r'^[-*]\s+', '', stripped), page_map, current_page)}</li>")
            i += 1
            continue

        if re.match(r"^\d+\.\s+", stripped):
            flush_paragraph()
            if ul_items:
                flush_lists()
            ol_items.append(f"<li>{inline_format(re.sub(r'^\d+\.\s+', '', stripped), page_map, current_page)}</li>")
            i += 1
            continue

        paragraph.append(stripped)
        i += 1

    flush_paragraph()
    flush_lists()
    if in_code:
        out.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
    return "\n".join(out), title, has_callout


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
        content, detected_title, has_callout = markdown_to_html(markdown_text, page_map, page)
        title = detected_title or page.title
        nav = build_nav(pages, page)
        root = root_prefix(page)
        content_class = " has-aside" if has_callout else ""
        rendered = (
            template.replace("{{title}}", html.escape(title))
            .replace("{{nav}}", nav)
            .replace("{{content}}", content)
            .replace("{{content_class}}", content_class)
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

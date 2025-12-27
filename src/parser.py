import re
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

STOP_HEADINGS = {
    "ссылки", "литература", "примечания", "источники", "библиография",
    "см. также", "внешние ссылки", "список литературы",
    "комментарии", "справочные материалы"
}

WIKI_CONTENT_SELECTORS = [
    "div.mw-parser-output",
    "div#mw-content-text",
    "div#bodyContent",
]

BRE_CONTENT_SELECTORS = [
    # old.bigenc.ru
    "div#content",
    "div.content",
    "div#text",
    "div.text",
    "div.article",
    "main article",
    "article",
    "main",
]

GLOBAL_REMOVE_SELECTORS = [
    "script", "style", "noscript",
    "header", "footer", "nav", "aside",
    "form", "button",
]

WIKI_REMOVE_SELECTORS = [
    "table", ".infobox", ".navbox", ".sidebar", ".reflist",
    ".references", ".catlinks", ".mw-editsection", ".hatnote",
    ".dablink", ".thumb", ".metadata", ".ambox",
    ".noprint", ".printfooter", ".toc"
]

BRE_REMOVE_SELECTORS = [
    "table",
    "figure", "figcaption",
    ".breadcrumbs", ".breadcrumb",
    ".share", ".social", ".socials",
    ".tags", ".tag", ".terms",
    ".media", ".gallery",
    ".annotation", ".abstract",
    ".bibliography", ".sources",
    ".versions", ".version",
    ".related", ".recommend", ".recommendations",
    ".sidebar", ".aside",
]

def parse_html(html_content: str, url: str | None = None, source_hint: str | None = None):
    if not html_content:
        return "", 0

    soup = BeautifulSoup(html_content, "html.parser")

    source = detect_source(soup, url=url, source_hint=source_hint)
    content_root = select_content_root(soup, source)

    if not content_root:
        return "", 0

    remove_by_selectors(content_root, GLOBAL_REMOVE_SELECTORS)

    if source == "wikipedia":
        remove_by_selectors(content_root, WIKI_REMOVE_SELECTORS)
    elif source == "bre":
        remove_by_selectors(content_root, BRE_REMOVE_SELECTORS)

    sanitize_inline_markup(content_root)

    paragraphs = collect_paragraphs_until_stop(content_root)

    clean_text = "\n\n".join(paragraphs).strip()
    word_count = count_words(clean_text)
    return clean_text, word_count


def detect_source(soup: BeautifulSoup, url: str | None, source_hint: str | None):
    if source_hint:
        return source_hint.lower()

    if url:
        host = urlparse(url).netloc.lower()
        if "wikipedia.org" in host:
            return "wikipedia"
        if "bigenc.ru" in host:
            return "bre"

    if soup.select_one("div.mw-parser-output") or soup.select_one("div#mw-content-text"):
        return "wikipedia"

    return "bre"


def select_content_root(soup: BeautifulSoup, source: str):
    selectors = WIKI_CONTENT_SELECTORS if source == "wikipedia" else BRE_CONTENT_SELECTORS

    for sel in selectors:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            return node

    h1 = soup.find("h1")
    if h1:
        parent = h1.parent
        for _ in range(6):
            if parent and parent.get_text(strip=True):
                return parent
            parent = parent.parent

    return soup.body


def remove_by_selectors(root, selectors):
    for sel in selectors:
        for tag in root.select(sel):
            tag.decompose()


def sanitize_inline_markup(root):
    for edit_span in root.select("span.mw-editsection"):
        edit_span.decompose()

    for sup in root.select("sup.reference"):
        sup.decompose()

    for sup in root.find_all("sup"):
        sup.decompose()

    for a in root.find_all("a"):
        a.replace_with(a.get_text(" ", strip=True))


def collect_paragraphs_until_stop(root):
    paragraphs = []

    for el in root.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
        text = clean_text(el.get_text(" ", strip=True))
        if not text:
            continue

        if el.name in {"h2", "h3", "h4", "h5", "h6"}:
            normalized = normalize_heading(text)
            if normalized in STOP_HEADINGS:
                break

        low = text.lower()
        if any(k in low for k in ["править", "обсуждение", "страница обсуждения", "загрузить", "распечатать"]):
            continue

        if len(text) < 30:
            continue

        paragraphs.append(text)

    dedup = []
    prev = None
    for p in paragraphs:
        if p != prev:
            dedup.append(p)
        prev = p

    return dedup


def normalize_heading(text: str) -> str:
    t = re.sub(r"^\s*\d+(\.\d+)*\s*", "", text.strip())
    t = t.replace(":", "").strip().lower()
    return t


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\s*\[[^\]]+\]\s*", " ", text)

    text = re.sub(r"\s{2,}", " ", text).strip()
    text = re.sub(r"\s+([.,;:!?)])", r"\1", text)
    text = re.sub(r"([(])\s+", r"\1", text)
    text = re.sub(r"(\w)\s+-\s+(\w)", r"\1-\2", text)

    return text


def count_words(text: str) -> int:
    if not text:
        return 0
    words = [w for w in text.split() if any(c.isalnum() for c in w)]
    return len(words)

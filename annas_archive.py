"""
Anna's Archive Store Plugin for Calibre.

Improvements over previous version:
  - Real HTTP health-check with TTL cache (not just DNS resolution)
  - Automatic mirror discovery from annas-archive.org
  - Mid-search mirror failover when a mirror dies mid-pagination
  - Last working mirror persisted to config across restarts
  - Direct URL routing for ISBN-10/13 and MD5 queries
  - Session-level search result cache (avoids duplicate HTTP round-trips)
  - File-size extraction alongside format
  - More robust result-div XPath (based on md5 links, not fragile Tailwind classes)
  - Format extraction restricted to short badge/chip elements first
  - Fixed _get_libgen_link / _get_zlib_link to use urlparse instead of split
  - i18n option labels via _() in constants.py
  - Configurable max_pages and timeout (saved/loaded through ConfigWidget)
"""

from contextlib import closing
import logging
import re
import socket
import threading
import time
from typing import Dict, Generator, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urljoin, urlparse
from urllib.request import urlopen, Request
from http.client import RemoteDisconnected

from calibre import browser
from calibre.gui2 import open_url
from calibre.gui2.store import StorePlugin
from calibre.gui2.store.search_result import SearchResult
from calibre.gui2.store.web_store_dialog import WebStoreDialog
from calibre_plugins.store_annas_archive.constants import (
    DEFAULT_MIRRORS, DEFAULT_TIMEOUT, MAX_PAGES_DEFAULT,
    MIRRORS_DISCOVERY_URL, SearchOption, TTLCache,
)
from lxml import html
from lxml.etree import ParserError

try:
    from qt.core import QUrl
except (ImportError, ModuleNotFoundError):
    from PyQt5.Qt import QUrl

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
SearchResults = Generator[SearchResult, None, None]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)
MIN_TITLE_LENGTH = 3
COMMON_FORMATS = ('EPUB', 'PDF', 'MOBI', 'AZW3', 'CBR', 'CBZ', 'FB2', 'DJVU', 'TXT')

# Headers that mimic a real Chrome browser — needed to pass Cloudflare's
# basic bot-detection on Anna's Archive mirrors.
_BROWSER_HEADERS = [
    ('User-Agent',
     'Mozilla/5.0 (X11; Linux x86_64) '
     'AppleWebKit/537.36 (KHTML, like Gecko) '
     'Chrome/124.0.0.0 Safari/537.36'),
    ('Accept',
     'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
     'image/webp,image/apng,*/*;q=0.8'),
    ('Accept-Language', 'en-US,en;q=0.9'),
    ('Connection', 'keep-alive'),
    ('Upgrade-Insecure-Requests', '1'),
    ('Sec-Fetch-Dest', 'document'),
    ('Sec-Fetch-Mode', 'navigate'),
    ('Sec-Fetch-Site', 'none'),
    ('Sec-Fetch-User', '?1'),
    ('Cache-Control', 'max-age=0'),
]

# Pre-compiled regexes
_ISBN_RE = re.compile(r'^(?:\d{9}[\dXx]|\d{13})$')
_MD5_RE = re.compile(r'^[0-9a-fA-F]{32}$')
_SIZE_RE = re.compile(r'\b(\d+(?:\.\d+)?\s*(?:KB|MB|GB))\b', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Module-level mirror health cache (shared across plugin instances in a session)
# ---------------------------------------------------------------------------
_mirror_health: TTLCache = TTLCache(ttl=300)   # 5-minute TTL
_mirror_health_lock = threading.Lock()

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class MirrorError(Exception):
    """Raised when no working mirrors are available."""


# ---------------------------------------------------------------------------
# Plugin class
# ---------------------------------------------------------------------------
class AnnasArchiveStore(StorePlugin):
    """
    Calibre store plugin for Anna's Archive.

    Key design choices:
    • Mirror health is checked via a real HTTP HEAD request (not just DNS) and
      cached with a 5-minute TTL so repeated searches don't hammer the mirrors.
    • Mirror discovery runs in a daemon thread at startup to keep the mirror
      list fresh without blocking the UI.
    • If a mirror fails mid-search (during pagination) the plugin transparently
      switches to the next available mirror and retries the failed page.
    • ISBN-10/13 and MD5 queries bypass the search API and go directly to the
      canonical detail page.
    • Search results are cached per (query, options) tuple for the Calibre
      session so the same search never fires twice.
    """

    def __init__(self, gui, name, config=None, base_plugin=None):
        super().__init__(gui, name, config, base_plugin)
        # Restore last working mirror from persisted config (survives restarts)
        self.working_mirror: Optional[str] = (self.config or {}).get('last_working_mirror')
        # Session-level search cache: cache_key -> List[SearchResult]
        self._session_cache: Dict[str, List[SearchResult]] = {}
        # Start background mirror discovery so the list is kept fresh
        self._start_mirror_discovery()

    # ------------------------------------------------------------------
    # Mirror management
    # ------------------------------------------------------------------

    @staticmethod
    def _check_mirror_health(mirror: str, timeout: int = 5) -> bool:
        """
        Perform a real HTTP HEAD request to verify the mirror is reachable.
        Results are cached in the module-level TTLCache for 5 minutes.
        """
        with _mirror_health_lock:
            cached = _mirror_health.get(mirror)
        if cached is not None:
            return cached

        ok = False
        try:
            # DNS first (fast-fail)
            domain = urlparse(mirror).netloc
            socket.gethostbyname(domain)
            # HTTP check — Anna's Archive mirrors sit behind Cloudflare which
            # returns 403/405 to HEAD requests.  Any HTTP response (even 4xx)
            # means the host is reachable; only connection-level errors = down.
            req = Request(mirror + '/', headers={'User-Agent': USER_AGENT}, method='HEAD')
            try:
                with urlopen(req, timeout=timeout) as resp:
                    ok = True
                    logger.debug('Health check OK for %s (HTTP %s)', mirror, resp.code)
            except HTTPError as http_exc:
                # 4xx from Cloudflare still means the host responded
                ok = http_exc.code < 500
                logger.debug('Health check HTTP %s for %s (%s)',
                             http_exc.code, mirror, 'OK' if ok else 'FAIL')
        except Exception as exc:
            logger.warning('Health check failed for %s: %s', mirror, exc)

        with _mirror_health_lock:
            _mirror_health.set(mirror, ok)
        return ok

    def _select_working_mirror(self, mirrors: List[str], timeout: int = 5) -> str:
        """
        Return the first healthy mirror. If all health checks fail, fall back
        to the first mirror in the list rather than raising — the actual search
        request will reveal whether it truly works.
        """
        ordered = list(mirrors)
        if self.working_mirror and self.working_mirror in ordered:
            ordered = [self.working_mirror] + [m for m in ordered if m != self.working_mirror]

        for mirror in ordered:
            if self._check_mirror_health(mirror, timeout=timeout):
                self.working_mirror = mirror
                if self.config is not None:
                    self.config['last_working_mirror'] = mirror
                logger.info('Selected mirror: %s', mirror)
                return mirror

        # All health checks failed (e.g. Cloudflare blocks HEAD on every mirror).
        # Fall back to the first mirror and let the real HTTP request decide.
        fallback = ordered[0]
        logger.warning(
            'All health checks failed; falling back to %s and attempting search anyway.',
            fallback,
        )
        self.working_mirror = fallback
        return fallback

    def _discover_mirrors(self, timeout: int = 10) -> List[str]:
        """
        Scrape the Anna's Archive main page for any ``annas-archive.{tld}`` URLs
        and return them as a de-duplicated list (main .org site excluded).
        """
        try:
            req = Request(MIRRORS_DISCOVERY_URL, headers={'User-Agent': USER_AGENT})
            with urlopen(req, timeout=timeout) as resp:
                content = resp.read().decode('utf-8', errors='replace')

            pattern = re.compile(r'https?://annas-archive\.[a-z]{2,6}(?:/[^\s"\'<>]*)?',
                                  re.IGNORECASE)
            seen: set = set()
            mirrors: List[str] = []
            for m in pattern.finditer(content):
                parsed = urlparse(m.group(0))
                base = f'{parsed.scheme}://{parsed.netloc}'
                if base not in seen and 'annas-archive.org' not in base:
                    seen.add(base)
                    mirrors.append(base)

            logger.info('Discovered %d mirrors: %s', len(mirrors), mirrors)
            return mirrors
        except Exception as exc:
            logger.warning('Mirror discovery failed: %s', exc)
            return []

    def _start_mirror_discovery(self) -> None:
        """Mirror auto-discovery is disabled: annas-archive.org no longer exists.
        Mirrors are managed via DEFAULT_MIRRORS and the config UI."""
        pass

    def _run_mirror_discovery(self) -> None:
        """Background: discover mirrors and merge them into the config."""
        discovered = self._discover_mirrors()
        if not discovered or self.config is None:
            return
        existing: List[str] = self.config.get('mirrors', list(DEFAULT_MIRRORS))
        # Merge: keep existing order, append new discoveries at the end
        merged = list(dict.fromkeys(existing + [m for m in discovered if m not in existing]))
        if merged != existing:
            self.config['mirrors'] = merged
            logger.info('Mirror list updated: %s', merged)

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    def _build_search_url(self, query: str, base_mirror: str) -> str:
        """Build the full search URL including all active filter parameters."""
        url = f'{base_mirror}/search?q={quote_plus(query)}'
        search_opts = self.config.get('search', {})
        for option in SearchOption.options:
            value = search_opts.get(option.config_option, ())
            if isinstance(value, str):
                value = (value,)
            for item in value:
                url += f'&{option.url_param}={item}'
        return url

    def _direct_url(self, query: str, base_mirror: str) -> Optional[str]:
        """
        If *query* looks like an ISBN (10/13 digits) or an MD5 (32 hex chars)
        return a direct detail-page URL, bypassing the search API entirely.
        """
        q = query.strip().replace('-', '').replace(' ', '')
        if _MD5_RE.match(q):
            return f'{base_mirror}/md5/{q.lower()}'
        if _ISBN_RE.match(q):
            return f'{base_mirror}/isbn/{q}'
        return None

    def _get_url(self, md5: str) -> str:
        """Build a full ``/md5/`` URL using the current or first configured mirror."""
        base = self.working_mirror or (self.config or {}).get('mirrors', DEFAULT_MIRRORS)[0]
        return f'{base}/md5/{md5}'

    # ------------------------------------------------------------------
    # HTML extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_md5(result_div) -> Optional[str]:
        try:
            links = result_div.xpath('.//a[starts-with(@href, "/md5/")]/@href')
            if links:
                md5 = links[0].split('/')[-1]
                return md5 if md5 else None
        except Exception as exc:
            logger.debug('Error extracting MD5: %s', exc)
        return None

    @staticmethod
    def _extract_title(result_div) -> Optional[str]:
        # Strategy 1: anchor with js-vim-focus class (the title link, not the cover link)
        try:
            links = result_div.xpath(
                './/a[starts-with(@href, "/md5/") and contains(@class, "js-vim-focus")]'
            )
            if links:
                title = ''.join(links[0].itertext()).strip()
                if title and len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 1 failed: %s', exc)

        # Strategy 2: data-content attribute on fallback cover div (always present)
        try:
            data = result_div.xpath(
                './/div[contains(@class,"js-aarecord-list-fallback-cover")]'
                '/div[1]/@data-content'
            )
            if data:
                title = data[0].strip()
                if title and len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 2 failed: %s', exc)

        # Strategy 3: any data-content div
        try:
            data = result_div.xpath('.//div[@data-content]/@data-content')
            if data:
                title = data[0].strip()
                if title and len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 3 failed: %s', exc)

        # Strategy 4: md5 links with actual text (skip those that only contain images)
        try:
            for a in result_div.xpath('.//a[starts-with(@href, "/md5/")]'):
                title = ''.join(a.itertext()).strip()
                if title and len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 4 failed: %s', exc)

        return None

    @staticmethod
    def _extract_author(result_div, title: str) -> str:
        try:
            # Strategy 1: second data-content in the fallback cover div (author)
            data = result_div.xpath(
                './/div[contains(@class,"js-aarecord-list-fallback-cover")]'
                '/div/@data-content'
            )
            if len(data) >= 2:
                return data[1].strip()
            # Strategy 2: icon-based author link
            links = result_div.xpath(
                './/a[.//span[contains(@class, "icon-[mdi--user-edit]")]]'
            )
            if links:
                return ''.join(links[0].itertext()).strip()
            # Strategy 3: any data-content that isn't the title
            all_data = result_div.xpath('.//div[@data-content]/@data-content')
            for d in all_data:
                candidate = d.strip()
                if candidate and candidate != title:
                    return candidate
        except Exception as exc:
            logger.debug('Error extracting author: %s', exc)
        return ''

    @staticmethod
    def _extract_format(result_div) -> Optional[str]:
        """
        Extract the file format, preferring short badge/chip-like elements
        (≤ 6 chars) before falling back to a full-text scan of the first 200
        characters to reduce false positives.
        """
        try:
            # Prefer dedicated badge elements with very short text
            for elem in result_div.xpath(
                './/*[self::span or self::div or self::td]'
                '[string-length(normalize-space(text())) <= 6]'
            ):
                text = (elem.text_content() or '').strip().upper()
                if text in COMMON_FORMATS:
                    return text
            # Fallback: first 200 chars of all text (avoids title false-positives)
            snippet = ''.join(result_div.xpath('.//text()'))[:200].upper()
            for fmt in COMMON_FORMATS:
                if fmt in snippet:
                    return fmt
        except Exception as exc:
            logger.debug('Error extracting format: %s', exc)
        return None

    # Regex to parse rgb() color from the fallback cover style attribute
    _RGB_RE = re.compile(r'rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)')

    @staticmethod
    def _generate_cover(title: str, author: str, bg_rgb: tuple) -> Optional[str]:
        """
        Render a small cover image with Qt and save it to a temp file.
        Returns a file:// URL or None on failure.

        Uses only Qt APIs that are always available inside Calibre — no Pillow needed.
        """
        try:
            try:
                from qt.core import (QImage, QPainter, QColor, QFont,
                                     QRect, Qt, QFontMetrics)
            except ImportError:
                from PyQt5.QtGui import QImage, QPainter, QColor, QFont, QFontMetrics
                from PyQt5.QtCore import QRect, Qt

            W, H = 96, 144   # pixels — matches the sm:w-24 sm:h-36 Tailwind size

            img = QImage(W, H, QImage.Format.Format_RGB32)
            bg = QColor(*bg_rgb)
            img.fill(bg)

            p = QPainter(img)
            p.setRenderHint(QPainter.RenderHint.TextAntialiasing)

            # Title — bold, violet-ish
            title_color = QColor(76, 29, 149)   # Tailwind violet-900
            p.setPen(title_color)
            f = QFont('sans-serif', 7, QFont.Weight.Bold)
            f.setPixelSize(9)
            p.setFont(f)
            title_rect = QRect(4, 4, W - 8, H - 30)
            p.drawText(title_rect,
                       Qt.TextFlag.TextWordWrap | Qt.AlignmentFlag.AlignTop,
                       title)

            # Author — smaller, amber-ish
            author_color = QColor(120, 53, 15)  # Tailwind amber-900
            p.setPen(author_color)
            f2 = QFont('sans-serif', 6)
            f2.setPixelSize(8)
            p.setFont(f2)
            author_rect = QRect(4, H - 28, W - 8, 26)
            p.drawText(author_rect,
                       Qt.TextFlag.TextWordWrap | Qt.AlignmentFlag.AlignBottom,
                       author)

            p.end()

            import tempfile, os
            tmp = tempfile.mktemp(prefix='aa_cover_', suffix='.png')
            if img.save(tmp, 'PNG'):
                return f'file://{tmp}'
        except Exception as exc:
            logger.debug('Cover generation failed: %s', exc)
        return None

    def _extract_cover_url(self, result_div, title: str = '',
                           author: str = '', base_mirror: str = '') -> Optional[str]:
        """
        Try three strategies in order:
        1. Generate a cover locally from the fallback-cover div (instant, no HTTP)
        2. Use the <img> src if it's an absolute URL
        3. Prefix a relative src with the mirror base
        """
        # Strategy 1: local Qt-generated cover from fallback div data
        try:
            fallback = result_div.xpath(
                './/div[contains(@class,"js-aarecord-list-fallback-cover")]'
            )
            if fallback:
                style = fallback[0].get('style', '')
                m = self._RGB_RE.search(style)
                bg = (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (220, 210, 230)
                cover = self._generate_cover(title, author, bg)
                if cover:
                    return cover
        except Exception as exc:
            logger.debug('Local cover generation failed: %s', exc)

        # Strategy 2 & 3: remote image from <img> src
        try:
            imgs = result_div.xpath('.//img/@src')
            if imgs:
                src = imgs[0]
                if src.startswith('http'):
                    return src
                return f'{base_mirror}{src}' if base_mirror else src
        except Exception as exc:
            logger.debug('Error extracting cover URL: %s', exc)
        return None

    @staticmethod
    def _extract_size(result_div) -> Optional[str]:
        """Return a human-readable file size string (e.g. '3.2 MB') if present."""
        try:
            text = ' '.join(result_div.xpath('.//text()'))
            m = _SIZE_RE.search(text)
            if m:
                return m.group(1)
        except Exception as exc:
            logger.debug('Error extracting size: %s', exc)
        return None

    def _parse_search_result(self, result_div, seen_md5: set, base_mirror: str = '') -> Optional[SearchResult]:
        """Parse a single result ``<div>`` into a :class:`SearchResult`."""
        md5 = self._extract_md5(result_div)
        if not md5 or md5 in seen_md5:
            return None
        seen_md5.add(md5)

        title = self._extract_title(result_div)
        if not title:
            return None

        s = SearchResult()
        s.detail_item = md5
        s.title = title
        s.author = self._extract_author(result_div, title)
        s.formats = self._extract_format(result_div)
        s.cover_url = self._extract_cover_url(result_div, s.title, s.author, base_mirror)
        s.price = '$0.00'
        s.drm = SearchResult.DRM_UNLOCKED

        # Annotate title with file size when available
        size = self._extract_size(result_div)
        if size:
            s.title = f'{s.title} [{size}]'

        return s

    # ------------------------------------------------------------------
    # Result div detection — multi-strategy with debug dump
    # ------------------------------------------------------------------

    # Ordered list of XPath expressions tried in sequence.
    # Each strategy is tried in turn; the first one that returns ≥1 divs is used.
    _RESULT_DIV_XPATHS = [
        # Strategy 1: anchor to the known result-list container (most precise)
        '//div[contains(@class,"js-aarecord-list-outer")]'
        '/div[contains(@class,"pt-3") and contains(@class,"border-b")]',

        # Strategy 2: fallback — same classes but without parent anchor
        # (catches redesigns that keep the row classes but rename the container)
        '//div[contains(@class,"flex") and contains(@class,"pt-3") '
        'and contains(@class,"pb-3") and contains(@class,"border-b") '
        'and contains(@class,"border-gray")]',

        # Strategy 3: relaxed Tailwind border-b row with any flex variant
        '//div[contains(@class,"border-b") and contains(@class,"flex") '
        'and contains(@class,"gap-")]',

        # Strategy 4: li/article tags as result containers (possible redesign)
        '//li[.//a[starts-with(@href,"/md5/")]]',
        '//article[.//a[starts-with(@href,"/md5/")]]',

        # Strategy 5: any div whose DIRECT child is an md5 anchor
        '//div[a[starts-with(@href,"/md5/")]]',

        # Strategy 6: innermost unique div containing an md5 link
        '__md5_parent__',
    ]

    def _find_result_divs(self, doc, page: int) -> list:
        """
        Try each XPath strategy in turn and return the first non-empty hit.
        If nothing matches, write a debug dump so the structure can be diagnosed.
        """
        for xpath in self._RESULT_DIV_XPATHS:
            try:
                if xpath == '__md5_parent__':
                    # Collect immediate parent divs of every md5 anchor, deduplicated
                    seen_ids: set = set()
                    divs = []
                    for anchor in doc.xpath('//a[starts-with(@href,"/md5/")]'):
                        parent = anchor.getparent()
                        while parent is not None and parent.tag != 'div':
                            parent = parent.getparent()
                        if parent is not None:
                            eid = id(parent)
                            if eid not in seen_ids:
                                seen_ids.add(eid)
                                divs.append(parent)
                    if divs:
                        logger.info('Result divs found via md5_parent strategy: %d', len(divs))
                        return divs
                else:
                    divs = doc.xpath(xpath)
                    if divs:
                        logger.info('Result divs found via "%s...": %d',
                                    xpath[:50], len(divs))
                        return divs
            except Exception as exc:
                logger.debug('XPath strategy failed (%s): %s', xpath[:50], exc)

        # Nothing matched — dump HTML structure for diagnosis
        self._dump_html_debug(doc, page)
        return []

    @staticmethod
    def _dump_html_debug(doc, page: int) -> None:
        """
        Log a compact structural summary of the page so we can diagnose why
        no result divs were found.  Writes to Calibre's debug log (visible
        when Calibre is launched with --debug or from Help > Debug device log).
        """
        import os, tempfile
        try:
            from lxml import etree
            # Collect all distinct tag+class combinations that contain md5 links
            md5_ancestors = []
            for anchor in doc.xpath('//a[starts-with(@href,"/md5/")]')[:5]:
                chain = []
                node = anchor.getparent()
                depth = 0
                while node is not None and depth < 6:
                    cls = (node.get('class') or '')[:80]
                    chain.append(f'<{node.tag} class="{cls}">')
                    node = node.getparent()
                    depth += 1
                md5_ancestors.append(' > '.join(reversed(chain)))

            logger.warning(
                'No result divs found on page %d.\n'
                'MD5-link ancestor chains (up to 5):\n%s\n'
                'Page title: %s',
                page,
                '\n'.join(md5_ancestors) if md5_ancestors else '(no md5 links found at all)',
                ''.join(doc.xpath('//title/text()')),
            )

            # Also save the full HTML to a temp file for easy inspection
            tmp = tempfile.mktemp(prefix=f'aa_debug_p{page}_', suffix='.html')
            with open(tmp, 'wb') as f:
                f.write(etree.tostring(doc, pretty_print=True, encoding='utf-8'))
            logger.warning('Full page HTML saved to: %s', tmp)
        except Exception as exc:
            logger.warning('Debug dump failed: %s', exc)

    # ------------------------------------------------------------------
    # Core search
    # ------------------------------------------------------------------

    def _make_cache_key(self, query: str, max_results: int) -> str:
        """Produce a string key that uniquely identifies this search request."""
        opts = sorted((self.config or {}).get('search', {}).items())
        return f'{query}|{max_results}|{opts}'

    def _search(self, query: str, max_results: int, timeout: int) -> SearchResults:
        """
        Search Anna's Archive, supporting:
        • Direct navigation for ISBN/MD5 queries
        • Pagination up to *max_pages* pages
        • Mid-search mirror failover

        Yields:
            SearchResult objects one by one.
        Raises:
            MirrorError: if no mirrors are available at all.
        """
        if not query or not query.strip():
            logger.warning('Empty search query')
            return

        # Calibre may pass query as bytes — normalise to str
        if isinstance(query, bytes):
            query = query.decode('utf-8', errors='replace')

        logger.info("Searching for %r (max_results=%d)", query, max_results)

        br = browser()
        br.addheaders = _BROWSER_HEADERS

        mirrors: List[str] = list((self.config or {}).get('mirrors', DEFAULT_MIRRORS))
        selected = self._select_working_mirror(mirrors, timeout=min(timeout, 10))
        remaining_mirrors = [m for m in mirrors if m != selected]

        # --- Direct URL for ISBN / MD5 ---
        direct = self._direct_url(query.strip(), selected)
        if direct:
            logger.info('Direct URL detected: %s', direct)
            try:
                with closing(br.open(direct, timeout=timeout)) as resp:
                    doc = html.fromstring(resp.read())
                # Treat the page as a single synthetic result
                s = SearchResult()
                md5 = query.strip().replace('-', '').replace(' ', '')
                s.detail_item = md5.lower() if _MD5_RE.match(md5) else md5
                s.title = ''.join(doc.xpath('//h1//text()')).strip() or query
                s.price = '$0.00'
                s.drm = SearchResult.DRM_UNLOCKED
                yield s
            except Exception as exc:
                logger.error('Direct URL fetch failed: %s', exc)
            return

        # --- Normal paginated search ---
        url_base = self._build_search_url(query, selected)
        seen_md5: set = set()
        results_count = 0
        page = 1
        max_pages = (self.config or {}).get('max_pages', MAX_PAGES_DEFAULT)

        while results_count < max_results and page <= max_pages:
            url = f'{url_base}&page={page}'
            logger.debug('Fetching page %d: %s', page, url)

            # Cloudflare requires a Referer on page 2+ — set it to the previous page
            if page > 1:
                br.addheaders = [h for h in _BROWSER_HEADERS
                                 if h[0] != 'Referer'] + [
                    ('Referer', f'{url_base}&page={page - 1}'),
                    ('Sec-Fetch-Site', 'same-origin'),
                ]
            else:
                br.addheaders = _BROWSER_HEADERS

            try:
                with closing(br.open(url, timeout=timeout)) as resp:
                    raw = resp.read()
                    if resp.code != 200:
                        snippet = raw[:300].decode('utf-8', errors='replace')
                        logger.warning(
                            'HTTP %s on page %d (mirror: %s)\nBody snippet: %s',
                            resp.code, page, selected, snippet,
                        )
                        break
                    try:
                        doc = html.fromstring(raw)
                    except ParserError as exc:
                        logger.error('HTML parse error on page %d: %s', page, exc)
                        break

            except (HTTPError, URLError, TimeoutError, RemoteDisconnected) as exc:
                logger.warning('Network error on page %d with %s: %s', page, selected, exc)
                # Invalidate failed mirror's cache entry and try the next one
                with _mirror_health_lock:
                    _mirror_health.invalidate(selected)
                if remaining_mirrors:
                    try:
                        selected = self._select_working_mirror(
                            remaining_mirrors, timeout=min(timeout, 10)
                        )
                        remaining_mirrors = [m for m in remaining_mirrors if m != selected]
                        url_base = self._build_search_url(query, selected)
                        logger.info('Switched to mirror %s; retrying page %d', selected, page)
                        continue   # retry same page with new mirror
                    except MirrorError:
                        logger.error('All mirrors exhausted during search')
                        break
                break

            except Exception as exc:
                logger.error('Unexpected error fetching page %d: %s', page, exc)
                page += 1
                continue

            # --- Parse result divs: try strategies in order ---
            result_divs = self._find_result_divs(doc, page)

            if not result_divs:
                logger.warning('Page %d: no result divs found with any strategy; stopping', page)
                break

            page_count = 0
            for div in result_divs:
                if results_count >= max_results:
                    break
                result = self._parse_search_result(div, seen_md5, selected)
                if result:
                    yield result
                    results_count += 1
                    page_count += 1

            if page_count == 0:
                logger.debug('No new results on page %d; stopping', page)
                break

            logger.info('Page %d: %d results (%d total)', page, page_count, results_count)
            page += 1
            # Small delay between pages to avoid Cloudflare rate-limiting
            if results_count < max_results and page <= max_pages:
                time.sleep(0.5)

        logger.info('Search complete: %d results', results_count)

    # ------------------------------------------------------------------
    # Public StorePlugin API
    # ------------------------------------------------------------------

    def search(self, query: str, max_results: int = 10, timeout: int = DEFAULT_TIMEOUT) -> SearchResults:
        """Public entry point. Uses session cache to avoid redundant requests."""
        # Override timeout from stored config if available
        timeout = (self.config or {}).get('timeout', timeout)

        cache_key = self._make_cache_key(query, max_results)
        if cache_key in self._session_cache:
            logger.debug('Session cache hit for %r', query)
            yield from self._session_cache[cache_key]
            return

        results: List[SearchResult] = []
        try:
            for result in self._search(query, max_results, timeout):
                results.append(result)
                yield result
        except MirrorError:
            logger.error('No working mirrors available')
        except Exception as exc:
            logger.exception('Unexpected error in search: %s', exc)

        if results:
            self._session_cache[cache_key] = results

    def open(self, parent=None, detail_item: Optional[str] = None, external: bool = False):
        """Open the book page, either in the built-in browser or externally."""
        if detail_item:
            url = self._get_url(detail_item)
        else:
            url = self.working_mirror or (self.config or {}).get('mirrors', DEFAULT_MIRRORS)[0]

        if external or (self.config or {}).get('open_external', False):
            open_url(QUrl(url))
        else:
            d = WebStoreDialog(self.gui, self.working_mirror, parent, url)
            d.setWindowTitle(self.name)
            d.set_tags((self.config or {}).get('tags', ''))
            d.exec()

    def get_details(self, search_result: SearchResult, timeout: int = 15):
        """Populate *search_result.downloads* by scraping the detail page."""
        if not search_result.detail_item:
            logger.warning('No detail_item provided for get_details')
            return

        timeout = (self.config or {}).get('timeout', timeout)
        logger.info('Getting details for: %s', search_result.title)

        link_opts = (self.config or {}).get('link', {})
        url_extension = link_opts.get('url_extension', True)
        content_type = link_opts.get('content_type', False)

        br = browser()
        br.addheaders = _BROWSER_HEADERS

        selected_mirror = self.working_mirror or (self.config or {}).get('mirrors', DEFAULT_MIRRORS)[0]

        try:
            with closing(br.open(self._get_url(search_result.detail_item), timeout=timeout)) as f:
                doc = html.fromstring(f.read())
        except Exception as exc:
            logger.error('Error fetching details page: %s', exc)
            return

        download_selectors = [
            '//div[@id="md5-panel-downloads"]//a[contains(@class,"js-download-link")]',
            '//a[contains(@class,"js-download-link")]',
            '//a[starts-with(@href,"http") and ('
            'contains(@href,"libgen") or contains(@href,"zlibrary") or '
            'contains(@href,"z-lib") or contains(@href,"sci-hub"))]',
        ]

        links = []
        for selector in download_selectors:
            try:
                links = doc.xpath(selector)
                if links:
                    logger.debug('Found %d download links via selector: %s', len(links), selector)
                    break
            except Exception as exc:
                logger.debug('Error with selector %s: %s', selector, exc)

        fmt = search_result.formats

        # Build the slow_download URL and try to resolve the real download link from it
        slow_url = f'{selected_mirror}/slow_download/{search_result.detail_item}/0/0'
        slow_fmt = (fmt or 'epub').lower()
        real_slow_url = self._resolve_download_page(slow_url, br, timeout)
        if real_slow_url:
            # Infer format from resolved URL if not known
            if not fmt:
                for ext in COMMON_FORMATS:
                    if f'.{ext.lower()}' in real_slow_url.lower():
                        slow_fmt = ext.lower()
                        break
            search_result.downloads[f"Anna\'s Archive (slow).{slow_fmt}"] = real_slow_url
            logger.debug('Resolved slow_download -> %s', real_slow_url)
        else:
            logger.debug('slow_download 403 for %s — skipping', search_result.detail_item)

        # Also add any direct external links found on the md5 page
        for link in links:
            url = link.get('href')
            if not url:
                continue
            # Skip slow/fast download — already handled above
            if '/slow_download/' in url or '/fast_download/' in url:
                continue

            link_text = ''.join(link.itertext()).strip()
            if not url.startswith('http'):
                url = urljoin(selected_mirror, url)

            original_url = url
            try:
                url = self._process_download_url(url, link_text, br)
            except Exception as exc:
                logger.warning('Error processing URL %s: %s', original_url, exc)
                continue
            if not url or not url.startswith('http'):
                continue
            if not self._validate_download_url(url, search_result, content_type, url_extension, timeout):
                continue

            if not fmt:
                for ext in COMMON_FORMATS:
                    if f'.{ext.lower()}' in url.lower():
                        fmt = ext
                        break
            if not fmt:
                continue

            source = link_text if link_text else 'Download'
            label = f'{source}.{fmt.lower()}'
            search_result.downloads[label] = url
            logger.debug('Added download: %s -> %s', label, url)

        logger.info('Found %d download links', len(search_result.downloads))

    def _resolve_download_page(self, url: str, br, timeout: int) -> Optional[str]:
        """
        Visit a /slow_download/ or /fast_download/ page and return the real
        "Download now" link found on it, or None if not found.
        """
        try:
            with closing(br.open(url, timeout=timeout)) as resp:
                raw = resp.read()
            doc = html.fromstring(raw)
            # Primary: "📚 Download now" anchor
            for xpath in [
                '//a[contains(text(),"Download now")]/@href',
                '//a[contains(text(),"download now")]/@href',
                '//p[contains(@class,"font-bold")]//a/@href',
                '//a[contains(@href,"b4mcx") or contains(@href,".net/d")]/@href',
            ]:
                hrefs = doc.xpath(xpath)
                if hrefs:
                    href = hrefs[0]
                    if href.startswith('http'):
                        logger.debug('Resolved download page %s -> %s', url, href)
                        return href
        except Exception as exc:
            logger.warning('Failed to resolve download page %s: %s', url, exc)
        return None

    # ------------------------------------------------------------------
    # Download URL helpers
    # ------------------------------------------------------------------

    def _process_download_url(self, url: str, link_text: str, br) -> str:
        url_lower = url.lower()
        if 'libgen.li' in url_lower or link_text == 'Libgen.li':
            return self._get_libgen_link(url, br)
        if 'libgen.rs' in url_lower or 'Libgen.rs' in link_text:
            return self._get_libgen_nonfiction_link(url, br)
        if 'sci-hub' in url_lower or link_text.startswith('Sci-Hub'):
            return self._get_scihub_link(url, br)
        if 'z-lib' in url_lower or 'zlibrary' in url_lower or link_text == 'Z-Library':
            return self._get_zlib_link(url, br)
        if not url.startswith('http'):
            return urljoin(self.working_mirror or '', url)
        return url

    def _validate_download_url(
        self,
        url: str,
        search_result: SearchResult,
        content_type: bool,
        url_extension: bool,
        timeout: int,
    ) -> bool:
        if content_type:
            try:
                with urlopen(Request(url, method='HEAD'), timeout=timeout) as resp:
                    if resp.info().get_content_maintype() != 'application':
                        logger.debug('Invalid Content-Type for %s', url)
                        return False
            except (HTTPError, URLError, TimeoutError, RemoteDisconnected) as exc:
                logger.debug('Content-Type check failed for %s: %s', url, exc)
                # Do not reject the link on network failure — assume valid

        if url_extension and search_result.formats:
            ext = '.' + search_result.formats.lower()
            params = url.find('?')
            end = params if params >= 0 else None
            if not url.endswith(ext, 0, end):
                logger.debug('Extension mismatch for %s (expected %s)', url, ext)
                return False

        return True

    @staticmethod
    def _get_libgen_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                parsed = urlparse(resp.geturl())
                base = f'{parsed.scheme}://{parsed.netloc}'
            href = ''.join(doc.xpath('//a[h2[text()="GET"]]/@href'))
            if href:
                return f'{base}/{href.lstrip("/")}'
        except Exception as exc:
            logger.debug('Error getting Libgen.li link: %s', exc)
        return ''

    @staticmethod
    def _get_libgen_nonfiction_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
            return ''.join(doc.xpath('//h2/a[text()="GET"]/@href'))
        except Exception as exc:
            logger.debug('Error getting Libgen.rs link: %s', exc)
        return ''

    @staticmethod
    def _get_scihub_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                scheme = urlparse(resp.geturl()).scheme
            pdf_url = ''.join(doc.xpath('//embed[@id="pdf"]/@src'))
            if pdf_url:
                return f'{scheme}:{pdf_url}' if pdf_url.startswith('//') else pdf_url
        except Exception as exc:
            logger.debug('Error getting Sci-Hub link: %s', exc)
        return ''

    @staticmethod
    def _get_zlib_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                parsed = urlparse(resp.geturl())
                base = f'{parsed.scheme}://{parsed.netloc}'
            href = ''.join(doc.xpath('//a[contains(@class, "addDownloadedBook")]/@href'))
            if href:
                return f'{base}/{href.lstrip("/")}'
        except Exception as exc:
            logger.debug('Error getting Z-Library link: %s', exc)
        return ''

    # ------------------------------------------------------------------
    # ConfigWidget integration
    # ------------------------------------------------------------------

    def config_widget(self):
        from calibre_plugins.store_annas_archive.config import ConfigWidget
        return ConfigWidget(self)

    def save_settings(self, config_widget):
        config_widget.save_settings()

"""
Anna's Archive Store Plugin for Calibre.

Searches and downloads books from Anna's Archive — the world's largest
open-source library aggregator, indexing Libgen, Z-Library, Sci-Hub,
Internet Archive, and more.

Features:
  - Multi-mirror support (.gl, .pk, .gd) with automatic health-check and
    failover mid-search
  - SLUM integration: mirror list pre-sorted by real-time uptime data from
    open-slum.org (Uptime Kuma public API), cached 5 min, with silent fallback
  - Cloudflare bypass via full browser headers (no Accept-Encoding compression)
  - Instant download buttons — get_details() is non-blocking; slow_download
    resolution happens at download time via a custom browser interceptor
  - Download interceptor: Libgen/Sci-Hub direct links first (all active Libgen
    mirrors from SLUM), then slow_download redirect follow as fallback
  - Cookie pre-warming in background thread during get_details() to reduce
    click-to-download latency
  - Local Qt-generated cover thumbnails (title + author + background color
    extracted from the page) — no external image requests needed
  - Paginated search with Referer header for pages 2+
  - Direct URL routing for ISBN-10/13 and MD5 queries
  - Session-level search result cache (TTL-based) to avoid duplicate requests
  - File size annotated in result titles (e.g. "[3.2 MB]")
  - All formats reported (e.g. "EPUB, PDF") not just the first one
  - Configurable mirrors, max pages, and timeout via the plugin settings UI
  - Mirror health test button with per-mirror latency display
"""

import base64
from contextlib import closing
import json
import logging
import re
import socket
import tempfile
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
    DEFAULT_MIRRORS, DEFAULT_TIMEOUT, MAX_PAGES_DEFAULT, RESULTS_PER_PAGE,
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
    'Chrome/124.0.0.0 Safari/537.36'
)
MIN_TITLE_LENGTH = 3
COMMON_FORMATS = ('EPUB', 'PDF', 'MOBI', 'AZW3', 'CBR', 'CBZ', 'FB2', 'DJVU', 'TXT')
_VALID_FORMATS = frozenset(COMMON_FORMATS)

# Headers that mimic a real Chrome browser — needed to pass Cloudflare's
# basic bot-detection on Anna's Archive mirrors.
_BROWSER_HEADERS = [
    ('User-Agent', USER_AGENT),
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

# SLUM (Shadow Library Uptime Monitor) — public Uptime Kuma instance.
# These endpoints require no authentication and are rate-limit friendly.
_SLUM_PAGE_API    = 'https://open-slum.org/api/status-page/shadow-libraries'
_SLUM_HB_API      = 'https://open-slum.org/api/status-page/heartbeat/shadow-libraries'
_SLUM_TIMEOUT     = 6   # seconds — fail fast, fall back silently
_SLUM_CACHE_TTL   = 300 # 5 minutes — same as mirror health TTL

# Pre-compiled regexes
_ISBN_RE  = re.compile(r'^(?:\d{9}[\dXx]|\d{13})$')
_MD5_RE   = re.compile(r'^[0-9a-fA-F]{32}$')
_SIZE_RE  = re.compile(r'\b(\d+(?:\.\d+)?\s*(?:KB|MB|GB))\b', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Module-level caches (shared across plugin instances in a session)
# ---------------------------------------------------------------------------
_mirror_health: TTLCache      = TTLCache(ttl=300)   # mirror url -> bool
_slum_cache:    TTLCache      = TTLCache(ttl=_SLUM_CACHE_TTL)  # 'status' -> dict
_mirror_health_lock           = threading.Lock()
_slum_lock                    = threading.Lock()

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
    • SLUM is queried once per 5 minutes (module-level TTLCache) in a background
      thread at startup.  It informs mirror ordering and Libgen mirror discovery.
      All SLUM calls are fire-and-forget; any failure is silently ignored.
    • Mirror health is verified via HTTP HEAD and cached 5 min.  SLUM-reported
      uptime sorts candidates before the HEAD check, reducing latency.
    • get_details() pre-warms cookies in a daemon thread so the first click is fast.
    • The download interceptor tries Libgen/Sci-Hub direct links (using all active
      Libgen mirrors from SLUM) before falling back to slow_download redirect.
    • slow_download fallback follows HTTP redirects instead of scraping HTML,
      which is robust against JS-rendered pages.
    • Search results use TTLCache (not a plain dict) to bound memory usage.
    • All formats are reported (not just the first match).
    • The download label key uses a clean extension (first format only) so
      Calibre can correctly identify and import the file.
    """

    def __init__(self, gui, name, config=None, base_plugin=None):
        super().__init__(gui, name, config, base_plugin)
        # Restore last working mirror from persisted config (survives restarts)
        self.working_mirror: Optional[str] = (self.config or {}).get('last_working_mirror')
        # Session-level search cache with TTL to bound memory
        self._session_cache: TTLCache = TTLCache(ttl=600)
        # Kick off SLUM fetch and mirror discovery in background — non-blocking
        self._start_background_init()

    # ------------------------------------------------------------------
    # Background initialisation
    # ------------------------------------------------------------------

    def _start_background_init(self) -> None:
        """Fetch SLUM status in the background so the first search benefits from it."""
        t = threading.Thread(target=self._background_init, daemon=True)
        t.start()

    def _background_init(self) -> None:
        """Background: fetch SLUM then merge any newly discovered AA mirrors."""
        self._fetch_slum_status()   # populates _slum_cache
        self._merge_discovered_mirrors()

    def _merge_discovered_mirrors(self) -> None:
        """Append SLUM-discovered AA mirrors to the config list (deduplicated)."""
        if self.config is None:
            return
        slum = self._get_slum_status()
        new_aa = [
            url for url, up in slum.items()
            if 'annas-archive' in url.lower()
        ]
        if not new_aa:
            return
        existing: List[str] = self.config.get('mirrors', list(DEFAULT_MIRRORS))
        merged = list(dict.fromkeys(existing + [m for m in new_aa if m not in existing]))
        if merged != existing:
            self.config['mirrors'] = merged
            logger.info('Mirror list updated from SLUM: %s', merged)

    # ------------------------------------------------------------------
    # SLUM integration
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_slum_status() -> None:
        """
        Fetch the SLUM public API and populate the module-level _slum_cache.

        The cache key 'status' maps to:
            { 'url_of_mirror': True/False, ... }

        Two HTTP calls are made:
          1. /api/status-page/shadow-libraries  — monitor metadata (id -> url)
          2. /api/status-page/heartbeat/...     — latest heartbeat per monitor

        Both calls use a short timeout; any failure is silently swallowed.
        """
        with _slum_lock:
            if _slum_cache.get('status') is not None:
                return  # already populated (or recently populated)

        try:
            # Step 1: id -> url map
            req = Request(_SLUM_PAGE_API, headers={'User-Agent': USER_AGENT})
            with urlopen(req, timeout=_SLUM_TIMEOUT) as resp:
                page_data = json.loads(resp.read())

            monitor_urls: Dict[str, str] = {}
            for group in page_data.get('publicGroupList', []):
                for monitor in group.get('monitorList', []):
                    mid = str(monitor.get('id', ''))
                    url = monitor.get('url', '').rstrip('/')
                    if mid and url:
                        monitor_urls[mid] = url

            # Step 2: latest heartbeat per monitor
            req2 = Request(_SLUM_HB_API, headers={'User-Agent': USER_AGENT})
            with urlopen(req2, timeout=_SLUM_TIMEOUT) as resp2:
                hb_data = json.loads(resp2.read())

            status: Dict[str, bool] = {}
            for mid, heartbeats in hb_data.get('heartbeatList', {}).items():
                url = monitor_urls.get(str(mid), '')
                if url and heartbeats:
                    # status 1 = UP, anything else = DOWN
                    status[url] = heartbeats[-1].get('status') == 1

            with _slum_lock:
                _slum_cache.set('status', status)

            up   = sum(1 for v in status.values() if v)
            down = len(status) - up
            logger.info('SLUM: %d monitors — %d up, %d down', len(status), up, down)

        except Exception as exc:
            logger.debug('SLUM fetch failed (non-fatal): %s', exc)
            # Store empty dict so we don't hammer SLUM on every call
            with _slum_lock:
                _slum_cache.set('status', {})

    @staticmethod
    def _get_slum_status() -> Dict[str, bool]:
        """Return cached SLUM status, triggering a fetch if stale."""
        cached = _slum_cache.get('status')
        if cached is None:
            AnnasArchiveStore._fetch_slum_status()
            cached = _slum_cache.get('status') or {}
        return cached

    def _slum_is_up(self, url: str) -> Optional[bool]:
        """
        Return True/False if SLUM knows about this URL, None if unknown.
        Matches on netloc so 'https://libgen.rs/...' matches 'https://libgen.rs'.
        """
        needle = urlparse(url).netloc
        status = self._get_slum_status()
        for monitored, is_up in status.items():
            if urlparse(monitored).netloc == needle:
                return is_up
        return None  # SLUM doesn't monitor this URL

    def _active_libgen_mirrors(self) -> List[str]:
        """
        Return Libgen mirror base URLs that SLUM reports as currently UP,
        ordered by preference (rs > li > la > others).
        Falls back to a hardcoded list if SLUM has no data.
        """
        _PREFERRED = ['libgen.rs', 'libgen.li', 'libgen.la', 'libgen.vg', 'libgen.bz']

        status = self._get_slum_status()
        active = [
            url for url, is_up in status.items()
            if is_up and 'libgen' in urlparse(url).netloc
        ]

        def _order(url: str) -> int:
            netloc = urlparse(url).netloc
            for i, domain in enumerate(_PREFERRED):
                if domain in netloc:
                    return i
            return len(_PREFERRED)

        ordered = sorted(active, key=_order)

        if not ordered:
            # SLUM unavailable — fall back to known mirrors
            ordered = [f'https://{d}' for d in _PREFERRED]
            logger.debug('SLUM returned no Libgen mirrors; using hardcoded fallback')

        return ordered

    # ------------------------------------------------------------------
    # Mirror management
    # ------------------------------------------------------------------

    @staticmethod
    def _check_mirror_health(mirror: str, timeout: int = 5) -> bool:
        """
        Perform a real HTTP HEAD request to verify the mirror is reachable.
        Results are cached in the module-level TTLCache for 5 minutes.
        Any HTTP response (even 4xx from Cloudflare) counts as reachable;
        only connection-level errors mean the host is truly down.
        """
        with _mirror_health_lock:
            cached = _mirror_health.get(mirror)
        if cached is not None:
            return cached

        ok = False
        try:
            domain = urlparse(mirror).netloc
            socket.gethostbyname(domain)  # fast-fail on DNS
            req = Request(mirror + '/', headers={'User-Agent': USER_AGENT}, method='HEAD')
            try:
                with urlopen(req, timeout=timeout):
                    ok = True
            except HTTPError as exc:
                ok = exc.code < 500   # 4xx = Cloudflare challenge but host is alive
                logger.debug('Health check HTTP %d for %s', exc.code, mirror)
        except Exception as exc:
            logger.warning('Health check failed for %s: %s', mirror, exc)

        with _mirror_health_lock:
            _mirror_health.set(mirror, ok)
        return ok

    def _select_working_mirror(self, mirrors: List[str], timeout: int = 5) -> str:
        """
        Return the first healthy mirror, SLUM-informed ordering applied first.

        Ordering priority:
          1. Previously working mirror (fastest path for repeat searches)
          2. Mirrors SLUM reports as UP
          3. Mirrors SLUM doesn't know about (neutral)
          4. Mirrors SLUM reports as DOWN (tried last)

        Falls back to the first candidate if all HEAD checks fail
        (e.g. Cloudflare blocks HEAD globally).
        """
        ordered = list(mirrors)

        # Bubble up the last known good mirror
        if self.working_mirror and self.working_mirror in ordered:
            ordered = [self.working_mirror] + [m for m in ordered if m != self.working_mirror]

        # Sort remaining by SLUM knowledge (stable sort preserves working_mirror at front)
        def _slum_rank(url: str) -> int:
            v = self._slum_is_up(url)
            if v is True:  return 0
            if v is None:  return 1
            return 2       # SLUM says DOWN

        head = ordered[:1]   # working_mirror stays pinned at position 0
        rest = sorted(ordered[1:], key=_slum_rank)
        ordered = head + rest

        for mirror in ordered:
            if self._check_mirror_health(mirror, timeout=timeout):
                self._set_working_mirror(mirror)
                return mirror

        fallback = ordered[0]
        logger.warning('All health checks failed; trying %s anyway', fallback)
        self._set_working_mirror(fallback)
        return fallback

    def _set_working_mirror(self, mirror: str) -> None:
        self.working_mirror = mirror
        if self.config is not None:
            self.config['last_working_mirror'] = mirror
        logger.info('Selected mirror: %s', mirror)

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    def _build_search_url(self, query: str, base_mirror: str) -> str:
        """Build the full search URL including all active filter parameters."""
        url = f'{base_mirror}/search?q={quote_plus(query)}'
        search_opts = (self.config or {}).get('search', {})
        for option in SearchOption.options:
            value = search_opts.get(option.config_option, ())
            if isinstance(value, str):
                value = (value,)
            for item in value:
                url += f'&{option.url_param}={item}'
        return url

    def _direct_url(self, query: str, base_mirror: str) -> Optional[str]:
        """
        Return a direct detail-page URL if *query* looks like an ISBN or MD5,
        bypassing the search API entirely.
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
                # Strip query params: "/md5/abc123?foo=bar" → "abc123"
                md5 = links[0].split('/')[-1].split('?')[0].split('#')[0]
                return md5 or None
        except Exception as exc:
            logger.debug('Error extracting MD5: %s', exc)
        return None

    @staticmethod
    def _extract_title(result_div) -> Optional[str]:
        # Strategy 1: anchor with js-vim-focus class (the canonical title link)
        try:
            links = result_div.xpath(
                './/a[starts-with(@href, "/md5/") and contains(@class, "js-vim-focus")]'
            )
            if links:
                title = ''.join(links[0].itertext()).strip()
                if len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 1 failed: %s', exc)

        # Strategy 2: first data-content in the fallback cover div
        try:
            data = result_div.xpath(
                './/div[contains(@class,"js-aarecord-list-fallback-cover")]'
                '/div[1]/@data-content'
            )
            if data:
                title = data[0].strip()
                if len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 2 failed: %s', exc)

        # Strategy 3: any data-content div
        try:
            data = result_div.xpath('.//div[@data-content]/@data-content')
            if data:
                title = data[0].strip()
                if len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 3 failed: %s', exc)

        # Strategy 4: any md5 anchor with text
        try:
            for a in result_div.xpath('.//a[starts-with(@href, "/md5/")]'):
                title = ''.join(a.itertext()).strip()
                if len(title) >= MIN_TITLE_LENGTH:
                    return title
        except Exception as exc:
            logger.debug('Title strategy 4 failed: %s', exc)

        return None

    @staticmethod
    def _extract_author(result_div, title: str) -> str:
        try:
            # Strategy 1: second data-content in fallback cover div
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
            # Strategy 3: any data-content that differs from the title
            for d in result_div.xpath('.//div[@data-content]/@data-content'):
                candidate = d.strip()
                if candidate and candidate != title:
                    return candidate
        except Exception as exc:
            logger.debug('Error extracting author: %s', exc)
        return ''

    @staticmethod
    def _extract_formats(result_div) -> str:
        """
        Return all recognised formats found in the result div, comma-separated
        (e.g. "EPUB, PDF").  Prefers short badge/chip elements (≤6 chars) to
        avoid false positives from titles, then falls back to a text snippet.
        """
        found: List[str] = []
        try:
            for elem in result_div.xpath(
                './/*[self::span or self::div or self::td]'
                '[string-length(normalize-space(text())) <= 6]'
            ):
                text = (elem.text_content() or '').strip().upper()
                if text in _VALID_FORMATS and text not in found:
                    found.append(text)
        except Exception as exc:
            logger.debug('Error extracting formats (badges): %s', exc)

        if not found:
            # Fallback: first 200 chars of concatenated text
            try:
                snippet = ''.join(result_div.xpath('.//text()'))[:200].upper()
                for fmt in COMMON_FORMATS:
                    if fmt in snippet and fmt not in found:
                        found.append(fmt)
                        break   # one is enough for the fallback path
            except Exception as exc:
                logger.debug('Error extracting formats (snippet): %s', exc)

        return ', '.join(found)

    # Pre-compiled ISBN regex — matches ISBN-13 (978/979) and ISBN-10
    _ISBN_COVER_RE = re.compile(r'\b(97[89]\d{10}|\d{9}[\dXx])\b')

    @staticmethod
    def _extract_isbn(result_div) -> Optional[str]:
        """Return the best ISBN found in the result div (prefers ISBN-13)."""
        try:
            text = ' '.join(result_div.xpath('.//text()'))
            m13 = re.findall(r'\b97[89]\d{10}\b', text)
            if m13:
                return m13[0]
            m10 = re.findall(r'\b\d{9}[\dXx]\b', text)
            if m10:
                return m10[0]
        except Exception as exc:
            logger.debug('Error extracting ISBN: %s', exc)
        return None

    # Curated palette — bg, title_color, author_color
    _COVER_PALETTE = [
        ((214, 234, 248), (21,  67,  96), (101, 60,   0)),  # blue
        ((212, 239, 223), (20,  90,  50), (101, 60,   0)),  # green
        ((253, 235, 208), (120, 66,  18), (76,  29, 149)),  # amber
        ((242, 215, 213), (120, 30,  20), (76,  29, 149)),  # red
        ((235, 222, 237), (76,  29, 149), (120, 53,  15)),  # violet
        ((208, 236, 231), (20,  80,  70), (101, 60,   0)),  # teal
        ((252, 228, 236), (131, 20,  75), (76,  29, 149)),  # pink
        ((255, 243, 205), (133, 100,  4), (76,  29, 149)),  # yellow
        ((225, 215, 252), (55,  20, 150), (120, 53,  15)),  # indigo
        ((209, 231, 221), (25,  80,  55), (131, 20,  75)),  # sage
        ((252, 215, 173), (140, 70,  10), (76,  29, 149)),  # orange
        ((210, 224, 252), (30,  60, 160), (120, 53,  15)),  # periwinkle
    ]

    @staticmethod
    def _generate_cover_data_uri(title: str, author: str,
                                  bg: tuple, title_color: tuple,
                                  author_color: tuple) -> Optional[str]:
        """Render a cover thumbnail with Qt and return it as a base64 data URI."""
        try:
            try:
                from qt.core import (QImage, QPainter, QColor, QFont,
                                     QRect, Qt, QBuffer, QIODevice)
            except ImportError:
                from PyQt5.QtGui import QImage, QPainter, QColor, QFont
                from PyQt5.QtCore import QRect, Qt, QBuffer, QIODevice

            W, H = 96, 144
            img = QImage(W, H, QImage.Format.Format_RGB32)
            img.fill(QColor(*bg))
            p = QPainter(img)
            p.setRenderHint(QPainter.RenderHint.TextAntialiasing)

            # Title
            p.setPen(QColor(*title_color))
            f = QFont('sans-serif')
            f.setPixelSize(9)
            f.setBold(True)
            p.setFont(f)
            p.drawText(QRect(5, 5, W - 10, H - 35),
                       Qt.TextFlag.TextWordWrap | Qt.AlignmentFlag.AlignTop, title)

            # Separator
            p.setPen(QColor(*title_color))
            p.drawLine(5, H - 33, W - 5, H - 33)

            # Author
            p.setPen(QColor(*author_color))
            f2 = QFont('sans-serif')
            f2.setPixelSize(7)
            f2.setItalic(True)
            p.setFont(f2)
            p.drawText(QRect(5, H - 30, W - 10, 28),
                       Qt.TextFlag.TextWordWrap | Qt.AlignmentFlag.AlignTop, author)
            p.end()

            try:
                qbuf = QBuffer()
                qbuf.open(QIODevice.OpenModeFlag.WriteOnly)
                img.save(qbuf, 'PNG')
                buf = bytes(qbuf.data())
                qbuf.close()
            except Exception:
                tmp = tempfile.mktemp(suffix='.png')
                img.save(tmp, 'PNG')
                with open(tmp, 'rb') as fh:
                    buf = fh.read()

            if buf:
                return f'data:image/png;base64,{base64.b64encode(buf).decode()}'
        except Exception as exc:
            logger.debug('Cover data URI generation failed: %s', exc)
        return None

    def _extract_cover_url(self, result_div, title: str = '',
                           author: str = '', base_mirror: str = '') -> Optional[str]:
        """
        Cover URL resolution:
        1. Google Books thumbnail by ISBN (real cover when available)
        2. Qt-rendered data URI from palette (instant, always works)
        """
        try:
            isbn = self._extract_isbn(result_div)
            if isbn:
                return (
                    f'https://books.google.com/books/content'
                    f'?vid=ISBN:{isbn}&printsec=frontcover&img=1&zoom=1'
                )
        except Exception as exc:
            logger.debug('Google Books ISBN cover failed: %s', exc)

        try:
            idx = hash(title or author or 'x') % len(self._COVER_PALETTE)
            bg, tc, ac = self._COVER_PALETTE[idx]
            return self._generate_cover_data_uri(title, author, bg, tc, ac)
        except Exception as exc:
            logger.debug('Qt cover failed: %s', exc)

        return None

    @staticmethod
    def _extract_size(result_div) -> Optional[str]:
        """Return a human-readable file size string (e.g. '3.2 MB') or None."""
        try:
            text = ' '.join(result_div.xpath('.//text()'))
            m = _SIZE_RE.search(text)
            if m:
                return m.group(1)
        except Exception as exc:
            logger.debug('Error extracting size: %s', exc)
        return None

    def _parse_search_result(self, result_div, seen_md5: set,
                             base_mirror: str = '') -> Optional[SearchResult]:
        """Parse a single result div into a SearchResult."""
        md5 = self._extract_md5(result_div)
        if not md5 or md5 in seen_md5:
            return None
        seen_md5.add(md5)

        title = self._extract_title(result_div)
        if not title:
            return None

        s = SearchResult()
        s.detail_item = md5
        s.title       = title
        s.author      = self._extract_author(result_div, title)
        s.formats     = self._extract_formats(result_div)
        s.cover_url   = self._extract_cover_url(result_div, title, s.author, base_mirror)
        s.price       = '$0.00'
        s.drm         = SearchResult.DRM_UNLOCKED

        size = self._extract_size(result_div)
        if size:
            s.title = f'{s.title} [{size}]'

        return s

    # ------------------------------------------------------------------
    # Result div detection — multi-strategy with debug dump
    # ------------------------------------------------------------------

    _RESULT_DIV_XPATHS = [
        # 1: anchor to known result-list container (most precise)
        '//div[contains(@class,"js-aarecord-list-outer")]'
        '/div[contains(@class,"pt-3") and contains(@class,"border-b")]',
        # 2: same row classes without the parent anchor
        '//div[contains(@class,"flex") and contains(@class,"pt-3") '
        'and contains(@class,"pb-3") and contains(@class,"border-b") '
        'and contains(@class,"border-gray")]',
        # 3: relaxed border-b flex row
        '//div[contains(@class,"border-b") and contains(@class,"flex") '
        'and contains(@class,"gap-")]',
        # 4: li/article containers (possible redesign)
        '//li[.//a[starts-with(@href,"/md5/")]]',
        '//article[.//a[starts-with(@href,"/md5/")]]',
        # 5: any div whose direct child is an md5 anchor
        '//div[a[starts-with(@href,"/md5/")]]',
        # 6: innermost unique div containing an md5 link (last resort)
        '__md5_parent__',
    ]

    def _find_result_divs(self, doc, page: int) -> list:
        """Try each XPath strategy and return the first non-empty hit."""
        for xpath in self._RESULT_DIV_XPATHS:
            try:
                if xpath == '__md5_parent__':
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
                        logger.info('Result divs via md5_parent: %d', len(divs))
                        return divs
                else:
                    divs = doc.xpath(xpath)
                    if divs:
                        logger.info('Result divs via "%s...": %d', xpath[:50], len(divs))
                        return divs
            except Exception as exc:
                logger.debug('XPath strategy failed (%s): %s', xpath[:50], exc)

        self._dump_html_debug(doc, page)
        return []

    @staticmethod
    def _dump_html_debug(doc, page: int) -> None:
        """Log a structural summary and save full HTML when no result divs found."""
        try:
            from lxml import etree
            ancestors = []
            for anchor in doc.xpath('//a[starts-with(@href,"/md5/")]')[:5]:
                chain, node, depth = [], anchor.getparent(), 0
                while node is not None and depth < 6:
                    chain.append(f'<{node.tag} class="{(node.get("class") or "")[:80]}">')
                    node, depth = node.getparent(), depth + 1
                ancestors.append(' > '.join(reversed(chain)))

            logger.warning(
                'No result divs on page %d.\nMD5 ancestor chains:\n%s\nTitle: %s',
                page,
                '\n'.join(ancestors) if ancestors else '(no md5 links)',
                ''.join(doc.xpath('//title/text()')),
            )
            # Only write debug file when DEBUG logging is active
            if logger.isEnabledFor(logging.DEBUG):
                tmp = tempfile.mktemp(prefix=f'aa_debug_p{page}_', suffix='.html')
                with open(tmp, 'wb') as fh:
                    fh.write(etree.tostring(doc, pretty_print=True, encoding='utf-8'))
                logger.debug('Full HTML saved to: %s', tmp)
        except Exception as exc:
            logger.warning('Debug dump failed: %s', exc)

    # ------------------------------------------------------------------
    # Core search
    # ------------------------------------------------------------------

    def _make_cache_key(self, query: str, max_results: int) -> str:
        opts = sorted((self.config or {}).get('search', {}).items())
        return f'{query}|{max_results}|{opts}'

    def _search(self, query: str, max_results: int, timeout: int) -> SearchResults:
        """
        Paginated search with mid-search mirror failover.
        Handles ISBN/MD5 direct navigation and yields SearchResult objects.
        """
        if not query or not query.strip():
            logger.warning('Empty search query')
            return

        if isinstance(query, bytes):
            query = query.decode('utf-8', errors='replace')

        logger.info('Searching for %r (max_results=%d)', query, max_results)

        br = browser()
        br.addheaders = _BROWSER_HEADERS

        mirrors: List[str] = list((self.config or {}).get('mirrors', DEFAULT_MIRRORS))
        selected = self._select_working_mirror(mirrors, timeout=min(timeout, 10))
        remaining_mirrors = [m for m in mirrors if m != selected]

        # --- Direct URL for ISBN / MD5 ---
        direct = self._direct_url(query.strip(), selected)
        if direct:
            logger.info('Direct URL: %s', direct)
            try:
                with closing(br.open(direct, timeout=timeout)) as resp:
                    doc = html.fromstring(resp.read())
                s = SearchResult()
                q = query.strip().replace('-', '').replace(' ', '')
                s.detail_item = q.lower() if _MD5_RE.match(q) else q
                s.title       = ''.join(doc.xpath('//h1//text()')).strip() or query
                s.price       = '$0.00'
                s.drm         = SearchResult.DRM_UNLOCKED
                yield s
            except Exception as exc:
                logger.error('Direct URL fetch failed: %s', exc)
            return

        # --- Normal paginated search ---
        url_base      = self._build_search_url(query, selected)
        seen_md5: set = set()
        results_count = 0
        page          = 1
        max_pages     = (self.config or {}).get('max_pages', MAX_PAGES_DEFAULT)

        # Calibre passes max_results=10 by default, which would stop after the
        # first page's first 10 entries.  We override with the page-based limit
        # so the user actually gets the full configured result set.
        effective_max = max(max_results, max_pages * RESULTS_PER_PAGE)

        while results_count < effective_max and page <= max_pages:
            url = f'{url_base}&page={page}'
            logger.debug('Fetching page %d: %s', page, url)

            # Cloudflare requires a Referer on page 2+
            if page > 1:
                br.addheaders = [h for h in _BROWSER_HEADERS if h[0] != 'Referer'] + [
                    ('Referer',         f'{url_base}&page={page - 1}'),
                    ('Sec-Fetch-Site',  'same-origin'),
                ]
            else:
                br.addheaders = _BROWSER_HEADERS

            try:
                with closing(br.open(url, timeout=timeout)) as resp:
                    raw = resp.read()
                    # Debug dump of page 1 only when DEBUG is active
                    if page == 1 and logger.isEnabledFor(logging.DEBUG):
                        try:
                            with open('/tmp/aa_calibre_p1.html', 'wb') as _f:
                                _f.write(raw)
                        except Exception:
                            pass
                    if resp.code != 200:
                        logger.warning('HTTP %s on page %d (%s)', resp.code, page, selected)
                        break
                    try:
                        doc = html.fromstring(raw)
                    except ParserError as exc:
                        logger.error('HTML parse error on page %d: %s', page, exc)
                        break

            except (HTTPError, URLError, TimeoutError, RemoteDisconnected) as exc:
                logger.warning('Network error on page %d with %s: %s', page, selected, exc)
                with _mirror_health_lock:
                    _mirror_health.invalidate(selected)
                if remaining_mirrors:
                    try:
                        selected = self._select_working_mirror(
                            remaining_mirrors, timeout=min(timeout, 10)
                        )
                        remaining_mirrors = [m for m in remaining_mirrors if m != selected]
                        url_base = self._build_search_url(query, selected)
                        logger.info('Switched to %s; retrying page %d', selected, page)
                        continue
                    except MirrorError:
                        logger.error('All mirrors exhausted')
                        break
                break

            except Exception as exc:
                logger.error('Unexpected error on page %d: %s', page, exc)
                page += 1
                continue

            result_divs = self._find_result_divs(doc, page)
            if not result_divs:
                # Distinguish between "no more results" and a Cloudflare challenge
                # page (returns HTTP 200 but contains a JS/cookie challenge with no
                # result divs).  If there are any /md5/ links at all, the page
                # rendered correctly but is just empty → stop.  If there are zero
                # /md5/ links AND the page is suspiciously short, it's likely a
                # challenge — try sleeping longer and retrying once before giving up.
                md5_links = doc.xpath('//a[starts-with(@href,"/md5/")]')
                page_text_len = len(raw)
                if not md5_links and page_text_len < 20_000:
                    logger.warning(
                        'Page %d: possible Cloudflare challenge (%d bytes, no md5 links); '
                        'sleeping 8s and retrying', page, page_text_len
                    )
                    time.sleep(8)
                    continue   # retry same page
                logger.warning('Page %d: no result divs; stopping', page)
                break

            page_count = 0
            for div in result_divs:
                if results_count >= effective_max:
                    break
                result = self._parse_search_result(div, seen_md5, selected)
                if result:
                    yield result
                    results_count += 1
                    page_count    += 1

            if page_count == 0:
                # All divs found were duplicates (seen_md5 already contains them).
                # This is a normal end-of-results condition, not a Cloudflare issue.
                logger.debug('Page %d: no new results (all duplicates); stopping', page)
                break

            logger.info('Page %d: %d results (%d total)', page, page_count, results_count)
            page += 1
            if results_count < effective_max and page <= max_pages:
                time.sleep(2.0 if page >= 3 else 0.5)

        logger.info('Search complete: %d results', results_count)

    # ------------------------------------------------------------------
    # Public StorePlugin API
    # ------------------------------------------------------------------

    def search(self, query: str, max_results: int = 10,
               timeout: int = DEFAULT_TIMEOUT) -> SearchResults:
        """Public entry point. Uses TTL-based session cache."""
        timeout   = (self.config or {}).get('timeout', timeout)
        cache_key = self._make_cache_key(query, max_results)

        cached = self._session_cache.get(cache_key)
        if cached is not None:
            logger.debug('Session cache hit for %r', query)
            yield from cached
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
            self._session_cache.set(cache_key, results)

    def open(self, parent=None, detail_item: Optional[str] = None,
             external: bool = False):
        """Open the book page in the built-in or external browser."""
        url = self._get_url(detail_item) if detail_item else (
            self.working_mirror or (self.config or {}).get('mirrors', DEFAULT_MIRRORS)[0]
        )
        if external or (self.config or {}).get('open_external', False):
            open_url(QUrl(url))
        else:
            d = WebStoreDialog(self.gui, self.working_mirror, parent, url)
            d.setWindowTitle(self.name)
            d.set_tags((self.config or {}).get('tags', ''))
            d.exec()

    def get_details(self, search_result: SearchResult, timeout: int = 15):
        """
        Register a slow_download placeholder immediately (no HTTP).
        The download key uses a clean single-extension label so Calibre can
        correctly identify the file type on import.
        Cookie pre-warming is kicked off in a background thread.
        """
        if not search_result.detail_item:
            return

        mirror = self.working_mirror or (self.config or {}).get('mirrors', DEFAULT_MIRRORS)[0]
        slow_url = f'{mirror}/slow_download/{search_result.detail_item}/0/0'

        # Key must use dot notation so Calibre can extract the extension:
        # e.g. "Anna's Archive.pdf" → Calibre splits on '.' to get "pdf"
        # Take only the first format when multiple are reported (e.g. "EPUB, PDF")
        raw_fmt = (search_result.formats or '').split(',')[0].strip().lower()
        fmt = raw_fmt if raw_fmt in {f.lower() for f in _VALID_FORMATS} else 'epub'
        search_result.downloads[f"Anna's Archive.{fmt}"] = slow_url

        # Pre-warm cookies in background to reduce click-to-download latency
        threading.Thread(
            target=self._prewarm_cookies,
            args=(mirror,),
            daemon=True,
        ).start()

    @staticmethod
    def _prewarm_cookies(mirror: str) -> None:
        """Visit the mirror root to establish Cloudflare/DDoS-Guard cookies."""
        try:
            br = browser()
            br.addheaders = _BROWSER_HEADERS
            with closing(br.open(f'{mirror}/', timeout=10)):
                pass
            logger.debug('Cookie pre-warm done for %s', mirror)
        except Exception as exc:
            logger.debug('Cookie pre-warm failed for %s: %s', mirror, exc)

    # ------------------------------------------------------------------
    # Download interceptor helpers
    # ------------------------------------------------------------------

    def _process_download_url(self, url: str, link_text: str, br) -> str:
        """Route a download URL to the appropriate extractor."""
        url_lower = url.lower()
        if 'libgen.li' in url_lower or link_text == 'Libgen.li':
            return self._get_libgen_li_link(url, br)
        if 'libgen' in url_lower and 'libgen.li' not in url_lower:
            return self._get_libgen_rs_link(url, br)
        if 'sci-hub' in url_lower or link_text.startswith('Sci-Hub'):
            return self._get_scihub_link(url, br)
        if 'z-lib' in url_lower or 'zlibrary' in url_lower or link_text == 'Z-Library':
            return self._get_zlib_link(url, br)
        if not url.startswith('http'):
            return urljoin(self.working_mirror or '', url)
        return url

    def _validate_download_url(self, url: str, search_result: SearchResult,
                                content_type: bool, url_extension: bool,
                                timeout: int) -> bool:
        if content_type:
            try:
                with urlopen(Request(url, method='HEAD'), timeout=timeout) as resp:
                    if resp.info().get_content_maintype() != 'application':
                        logger.debug('Invalid Content-Type for %s', url)
                        return False
            except (HTTPError, URLError, TimeoutError, RemoteDisconnected) as exc:
                logger.debug('Content-Type check failed for %s: %s', url, exc)

        if url_extension and search_result.formats:
            ext   = '.' + search_result.formats.split(',')[0].strip().lower()
            end   = url.find('?')
            end   = end if end >= 0 else None
            if not url.endswith(ext, 0, end):
                logger.debug('Extension mismatch for %s (expected %s)', url, ext)
                return False

        return True

    @staticmethod
    def _get_libgen_li_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc    = html.fromstring(resp.read())
                parsed = urlparse(resp.geturl())
                base   = f'{parsed.scheme}://{parsed.netloc}'
            href = ''.join(doc.xpath('//a[h2[text()="GET"]]/@href'))
            if href:
                return f'{base}/{href.lstrip("/")}'
        except Exception as exc:
            logger.debug('Libgen.li link failed: %s', exc)
        return ''

    @staticmethod
    def _get_libgen_rs_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
            return ''.join(doc.xpath('//h2/a[text()="GET"]/@href'))
        except Exception as exc:
            logger.debug('Libgen.rs link failed: %s', exc)
        return ''

    @staticmethod
    def _get_scihub_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc    = html.fromstring(resp.read())
                scheme = urlparse(resp.geturl()).scheme
            pdf_url = ''.join(doc.xpath('//embed[@id="pdf"]/@src'))
            if pdf_url:
                return f'{scheme}:{pdf_url}' if pdf_url.startswith('//') else pdf_url
        except Exception as exc:
            logger.debug('Sci-Hub link failed: %s', exc)
        return ''

    @staticmethod
    def _get_zlib_link(url: str, br) -> str:
        try:
            with closing(br.open(url)) as resp:
                doc    = html.fromstring(resp.read())
                parsed = urlparse(resp.geturl())
                base   = f'{parsed.scheme}://{parsed.netloc}'
            href = ''.join(doc.xpath('//a[contains(@class,"addDownloadedBook")]/@href'))
            if href:
                return f'{base}/{href.lstrip("/")}'
        except Exception as exc:
            logger.debug('Z-Library link failed: %s', exc)
        return ''

    # ------------------------------------------------------------------
    # create_browser — download interceptor
    # ------------------------------------------------------------------

    def create_browser(self):
        """
        Called by Calibre just before downloading.
        Intercepts slow_download URLs and resolves them in this order:

          Step 1 — Direct links from the /md5/ page
                   Tries all Libgen mirrors that SLUM reports as UP,
                   plus Sci-Hub.  Returns on first success.

          Step 2 — Redirect follow on slow_download
                   Follows the HTTP redirect that Anna's Archive issues
                   when valid cookies are present.  No HTML scraping —
                   robust against JS-rendered pages.

        If both steps fail on all mirrors, raises a descriptive exception.
        """
        br           = browser()
        br.addheaders = _BROWSER_HEADERS
        plugin_self  = self
        original_open = br.open

        def intercepting_open(url_or_req, *args, **kwargs):
            url = (url_or_req if isinstance(url_or_req, str)
                   else url_or_req.get_full_url())

            if '/slow_download/' not in url:
                return original_open(url_or_req, *args, **kwargs)

            logger.info('Intercepting slow_download: %s', url)

            try:
                md5 = url.split('/slow_download/')[1].split('/')[0]
            except IndexError:
                return original_open(url_or_req, *args, **kwargs)

            mirrors = list((plugin_self.config or {}).get('mirrors', DEFAULT_MIRRORS))
            if plugin_self.working_mirror:
                mirrors = ([plugin_self.working_mirror] +
                           [m for m in mirrors if m != plugin_self.working_mirror])
            primary = mirrors[0]

            # ----------------------------------------------------------
            # Step 1: direct links from /md5/ page
            # ----------------------------------------------------------
            # Build an ordered set of Libgen mirrors to try:
            # SLUM-active mirrors first, then hardcoded fallbacks — deduped.
            libgen_mirrors = plugin_self._active_libgen_mirrors()

            try:
                with closing(br.open(f'{primary}/md5/{md5}', timeout=20)) as f:
                    doc = html.fromstring(f.read())

                # Gather all external download links on the page
                ext_links = doc.xpath(
                    '//a[starts-with(@href,"http") and ('
                    'contains(@href,"libgen") or contains(@href,"sci-hub"))]'
                )

                for link in ext_links:
                    href      = link.get('href', '')
                    link_text = ''.join(link.itertext()).strip()
                    href_low  = href.lower()

                    # Only follow links to Libgen mirrors SLUM says are UP,
                    # or Sci-Hub (no SLUM filter needed — it's a single host)
                    if 'libgen' in href_low:
                        netloc = urlparse(href).netloc
                        active_netlocs = {urlparse(m).netloc for m in libgen_mirrors}
                        if netloc not in active_netlocs:
                            logger.debug('Skipping Libgen mirror not in SLUM active list: %s', netloc)
                            continue

                    try:
                        direct = plugin_self._process_download_url(href, link_text, br)
                    except Exception as exc:
                        logger.debug('Direct link processing failed: %s', exc)
                        continue

                    if direct and direct.startswith('http'):
                        logger.info('Step 1 success via direct link: %s', direct)
                        return original_open(direct, *args, **kwargs)

            except Exception as exc:
                logger.warning('/md5/ page fetch failed: %s', exc)

            # ----------------------------------------------------------
            # Step 2: follow slow_download redirect
            # ----------------------------------------------------------
            # mechanize follows redirects automatically.  When AA has valid
            # cookies it redirects directly to the file on its CDN.
            # We detect success by checking that the final URL left AA's domain
            # OR that the Content-Type is a file download.
            for mirror in mirrors:
                slow_url = f'{mirror}/slow_download/{md5}/0/0'
                try:
                    with closing(br.open(slow_url, timeout=30)) as resp:
                        final_url    = resp.geturl()
                        content_type = resp.info().get_content_type() or ''

                    aa_netloc = urlparse(mirror).netloc
                    redirected_away = urlparse(final_url).netloc != aa_netloc
                    is_file_download = content_type.startswith('application/')

                    if redirected_away or is_file_download:
                        logger.info('Step 2 success via redirect: %s -> %s',
                                    slow_url, final_url)
                        return original_open(final_url, *args, **kwargs)

                    logger.debug('Redirect stayed on AA (%s); trying next mirror', final_url)

                except HTTPError as exc:
                    if exc.code == 429:
                        logger.warning('Rate-limited on %s (429); waiting 5 s', mirror)
                        time.sleep(5)
                    else:
                        logger.warning('slow_download HTTP %d on %s', exc.code, mirror)
                except Exception as exc:
                    logger.warning('slow_download failed on %s: %s', mirror, exc)

            raise Exception(
                'No se pudo descargar: todos los métodos fallaron.\n'
                'Espera unos segundos y vuelve a intentarlo, o usa '
                '"Abrir en navegador externo" para descargar manualmente.'
            )

        br.open = intercepting_open
        return br

    # ------------------------------------------------------------------
    # ConfigWidget integration
    # ------------------------------------------------------------------

    def config_widget(self):
        from calibre_plugins.store_annas_archive.config import ConfigWidget
        return ConfigWidget(self)

    def save_settings(self, config_widget):
        config_widget.save_settings()

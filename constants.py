"""
Constants and configuration data structures for the Anna's Archive Calibre plugin.
"""

from collections import OrderedDict
import logging
import time
from typing import Dict, Iterable, List, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from qt.core import QCheckBox, QComboBox

# ---------------------------------------------------------------------------
# i18n bootstrap – must happen before any _() call at module level
# ---------------------------------------------------------------------------
try:
    load_translations()
except NameError:
    # Outside Calibre (unit-tests, standalone runs)
    def _(s: str) -> str:  # noqa: E731
        return s

logger = logging.getLogger(__name__)

__all__ = (
    'DEFAULT_MIRRORS', 'MIRRORS_DISCOVERY_URL', 'MAX_PAGES_DEFAULT', 'DEFAULT_TIMEOUT',
    'TTLCache', 'SearchOption', 'SearchConfiguration', 'CheckboxConfiguration',
    'Order', 'Content', 'Access', 'FileType', 'Source', 'Language',
)

# ---------------------------------------------------------------------------
# Mirror constants
# ---------------------------------------------------------------------------
DEFAULT_MIRRORS = [
    'https://annas-archive.gl',
    'https://annas-archive.pk',
    'https://annas-archive.gd',
]
# Discovery URL intentionally removed: annas-archive.org is no longer active.
# Mirror list must be maintained manually or via the plugin config UI.
MIRRORS_DISCOVERY_URL = ''
RESULTS_PER_PAGE = 100

# ---------------------------------------------------------------------------
# Tuneable defaults (exposed in ConfigWidget)
# ---------------------------------------------------------------------------
MAX_PAGES_DEFAULT = 20
DEFAULT_TIMEOUT = 60


# ---------------------------------------------------------------------------
# TTL-based in-memory cache
# ---------------------------------------------------------------------------
class TTLCache:
    """Simple TTL key-value cache (no external dependencies)."""

    def __init__(self, ttl: int = 300) -> None:
        self._cache: Dict[str, Tuple] = {}
        self._ttl = ttl

    def get(self, key: str):
        """Return cached value or *None* if expired or missing."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        value, timestamp = entry
        if time.time() - timestamp > self._ttl:
            del self._cache[key]
            return None
        return value

    def set(self, key: str, value) -> None:
        """Store *value* under *key* with the current timestamp."""
        self._cache[key] = (value, time.time())

    def invalidate(self, key: str) -> None:
        """Remove *key* from the cache (no-op if absent)."""
        self._cache.pop(key, None)

    def clear(self) -> None:
        """Remove all entries."""
        self._cache.clear()


# ---------------------------------------------------------------------------
# Search option meta-class and configuration helpers
# ---------------------------------------------------------------------------
class SearchOption(type):
    """Metaclass / factory that produces SearchConfiguration subclasses."""

    options: List[Type['SearchConfiguration']] = []

    def __new__(
        mcs,
        name: str,
        config_option: str,
        url_param: str,
        base: 'SearchConfiguration',
        options: Iterable[Tuple[str, str]],
    ):
        values = tuple(option[1] for option in options)
        cls = super().__new__(
            mcs, name, (base,),
            {
                'name': name,
                'config_option': config_option,
                'url_param': url_param,
                'options': options,
                'values': values,
            },
        )
        mcs.options.append(cls)
        return cls

    def __init__(
        cls,
        name: str,
        config_option: str,
        url_param: str,
        base: 'SearchConfiguration',
        options: Iterable[Tuple[str, str]],
    ):
        super().__init__(cls)


class SearchConfiguration:
    name: str
    config_option: str
    url_param: str
    options: Iterable[Tuple[str, str]]
    values: Tuple[str, ...]
    default = ''

    def __init__(self, combo_box):
        self.combo_box: 'QComboBox' = combo_box

    def to_save(self):
        return self.combo_box.currentData()

    def load(self, value: str) -> None:
        if value in self.values:
            self.combo_box.setCurrentIndex(self.values.index(value))
        else:
            logger.warning(
                "Config value %r not found in '%s' options; falling back to default.",
                value, self.name,
            )
            self.combo_box.setCurrentIndex(0)


class CheckboxConfiguration(SearchConfiguration):
    default: list = []

    def __init__(self):
        self.checkboxes: Dict[str, 'QCheckBox'] = {}

    def to_save(self):
        return [type_ for type_, cbx in self.checkboxes.items() if cbx.isChecked()]

    def load(self, value) -> None:
        for type_ in value:
            if type_ in self.checkboxes:
                self.checkboxes[type_].setChecked(True)
            else:
                logger.warning(
                    "Config value %r no longer exists in '%s' options; skipping.",
                    type_, self.__class__.__name__,
                )


# ---------------------------------------------------------------------------
# Concrete search options (display labels wrapped with _() for i18n)
# ---------------------------------------------------------------------------
Order = SearchOption('Order', 'order', 'sort', SearchConfiguration, (
    (_('Most relevant'), ''),
    (_('Newest (publication year)'), 'newest'),
    (_('Oldest (publication year)'), 'oldest'),
    (_('Largest'), 'largest'),
    (_('Smallest'), 'smallest'),
    (_('Newest (open sourced)'), 'newest_added'),
    (_('Oldest (open sourced)'), 'oldest_added'),
))

Content = SearchOption('Content', 'content', 'content', CheckboxConfiguration, (
    (_('Book (non-fiction)'), 'book_nonfiction'),
    (_('Book (fiction)'), 'book_fiction'),
    (_('Book (unknown)'), 'book_unknown'),
    (_('Magazine'), 'magazine'),
    (_('Comic book'), 'book_comic'),
    (_('Standards Document'), 'standards_document'),
    (_('Other'), 'other'),
    (_('Musical score'), 'musical_score'),
    (_('Audiobook'), 'audiobook'),
))

Access = SearchOption('Access', 'access', 'acc', CheckboxConfiguration, (
    (_('Partner Server download'), 'aa_download'),
    (_('External download'), 'external_download'),
    (_('External borrow'), 'external_borrow'),
    (_('External borrow (print disabled)'), 'external_borrow_printdisabled'),
    (_('Contained in torrents'), 'torrents_available'),
))

FileType = SearchOption('Filetype', 'filetype', 'ext', CheckboxConfiguration, tuple(zip(
    *((('epub', 'mobi', 'pdf', 'azw3', 'cbr', 'cbz', 'fb2', 'djvu', 'txt'),) * 2)
)))

Source = SearchOption('Source', 'source', 'src', CheckboxConfiguration, (
    ('Libgen.li', 'lgli'),
    ('Libgen.rs', 'lgrs'),
    ('Sci-Hub', 'scihub'),
    ('Z-Library', 'zlib'),
    ('Internet Archive', 'ia'),
    (_('Uploads to AA'), 'upload'),
    ('Nexus/STC', 'nexusstc'),
    ('DuXiu', 'duxiu'),
    (_('Z-Library Chinese'), 'zlibzh'),
    ('MagzDB', 'magzdb'),
))

_languages = OrderedDict({
    _('Unknown language'): '_empty',
    'English': 'en', 'Spanish': 'es', 'Italian': 'it', 'Portuguese': 'pt',
    'French': 'fr', 'German': 'de', 'Chinese': 'zh', 'Turkish': 'tr',
    'Dutch': 'nl', 'Hungarian': 'hu', 'Catalan': 'ca', 'Romanian': 'ro',
    'Russian': 'ru', 'Czech': 'cs', 'Lithuanian': 'lt', 'Greek': 'el',
    'Polish': 'pl', 'Danish': 'da', 'Croatian': 'hr', 'Korean': 'ko',
    'Hindi': 'hi', 'Japanese': 'ja', 'Latvian': 'lv', 'Latin': 'la',
    'Indonesian': 'id', 'Swedish': 'sv', 'Hebrew': 'he', 'Bangla': 'bn',
    'Norwegian': 'no', 'Ukrainian': 'uk', 'Luxembourgish': 'lb', 'Arabic': 'ar',
    'Irish': 'ga', 'Welsh': 'cy', 'Bulgarian': 'bg', 'Tamil': 'ta',
    'Traditional Chinese': 'zh-Hant', 'Afrikaans': 'af', 'Persian': 'fa',
    'Serbian': 'sr', 'Belarusian': 'be', 'Dongxiang': 'sce', 'Vietnamese': 'vi',
    'Urdu': 'ur', 'Flemish': 'nl-BE', 'Ndolo': 'ndl', 'Kazakh': 'kk',
})
Language = SearchOption('Language', 'language', 'lang', CheckboxConfiguration, tuple(
    (f"{name} [{code}]" if code != '_empty' else name, code)
    for name, code in _languages.items()
))

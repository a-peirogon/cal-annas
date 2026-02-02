"""
Anna's Archive Store Plugin for Calibre
Improved version with better error handling, logging, and performance optimizations.
"""

from contextlib import closing
from functools import lru_cache
from http.client import RemoteDisconnected
import logging
import socket
from typing import Generator, Optional, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urljoin
from urllib.request import urlopen, Request

from calibre import browser
from calibre.gui2 import open_url
from calibre.gui2.store import StorePlugin
from calibre.gui2.store.search_result import SearchResult
from calibre.gui2.store.web_store_dialog import WebStoreDialog
from calibre_plugins.store_annas_archive.constants import DEFAULT_MIRRORS, SearchOption
from lxml import html
from lxml.etree import ParserError

try:
    from qt.core import QUrl
except (ImportError, ModuleNotFoundError):
    from PyQt5.Qt import QUrl

# Type aliases
SearchResults = Generator[SearchResult, None, None]

# Constants
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
MAX_PAGES_DEFAULT = 20
MIN_TITLE_LENGTH = 3
DNS_CACHE_SIZE = 128
COMMON_FORMATS = ('EPUB', 'PDF', 'MOBI', 'AZW3', 'CBR', 'CBZ', 'FB2', 'DJVU', 'TXT')

# Configure logging
logger = logging.getLogger(__name__)


class MirrorError(Exception):
    """Raised when no working mirrors are available."""
    pass


class AnnasArchiveStore(StorePlugin):
    """
    Calibre plugin for searching and downloading books from Anna's Archive.

    Features:
    - Multi-mirror support with automatic failover
    - DNS verification and caching
    - Robust HTML parsing with multiple fallback strategies
    - Support for multiple download sources (Libgen, Z-Library, Sci-Hub, etc.)
    """

    def __init__(self, gui, name, config=None, base_plugin=None):
        super().__init__(gui, name, config, base_plugin)
        self.working_mirror: Optional[str] = None
        self._dns_cache: dict = {}

    @lru_cache(maxsize=DNS_CACHE_SIZE)
    def _check_dns(self, mirror: str) -> bool:
        """
        Verifica si el dominio del mirror tiene resolución DNS.

        Args:
            mirror: URL del mirror a verificar

        Returns:
            True si el dominio resuelve, False en caso contrario
        """
        try:
            domain = mirror.split("//")[1].split("/")[0]
            socket.gethostbyname(domain)
            logger.debug(f"DNS check passed for {domain}")
            return True
        except (socket.gaierror, IndexError) as e:
            logger.warning(f"DNS check failed for {mirror}: {e}")
            return False

    def _select_working_mirror(self, mirrors: List[str]) -> str:
        """
        Selecciona un mirror funcional de la lista.

        Args:
            mirrors: Lista de URLs de mirrors

        Returns:
            URL del primer mirror funcional

        Raises:
            MirrorError: Si ningún mirror está disponible
        """
        # Priorizar el mirror que funcionó anteriormente
        if self.working_mirror and self.working_mirror in mirrors:
            mirrors = [self.working_mirror] + [m for m in mirrors if m != self.working_mirror]

        for mirror in mirrors:
            if self._check_dns(mirror):
                self.working_mirror = mirror
                logger.info(f"Selected working mirror: {mirror}")
                return mirror

        self.working_mirror = None
        raise MirrorError('No working mirrors of Anna\'s Archive found.')

    def _build_search_url(self, query: str, base_mirror: str) -> str:
        """
        Construye la URL de búsqueda con todos los parámetros.

        Args:
            query: Término de búsqueda
            base_mirror: URL base del mirror

        Returns:
            URL completa de búsqueda
        """
        url = f'{base_mirror}/search?q={quote_plus(query)}'
        search_opts = self.config.get('search', {})

        for option in SearchOption.options:
            value = search_opts.get(option.config_option, ())
            if isinstance(value, str):
                value = (value,)
            for item in value:
                url += f'&{option.url_param}={item}'

        return url

    def _extract_md5(self, result_div) -> Optional[str]:
        """
        Extrae el MD5 de un div de resultado.

        Args:
            result_div: Elemento HTML del resultado

        Returns:
            MD5 string o None si no se encuentra
        """
        try:
            md5_links = result_div.xpath('.//a[starts-with(@href, "/md5/")]/@href')
            if md5_links:
                href = md5_links[0]
                md5 = href.split('/')[-1] if href else None
                return md5 if md5 else None
        except Exception as e:
            logger.debug(f"Error extracting MD5: {e}")
        return None

    def _extract_title(self, result_div) -> Optional[str]:
        """
        Extrae el título usando múltiples estrategias de fallback.

        Args:
            result_div: Elemento HTML del resultado

        Returns:
            Título o None si no se encuentra
        """
        title = None

        # Estrategia 1: Buscar en el enlace con clase js-vim-focus
        try:
            title_links = result_div.xpath(
                './/a[starts-with(@href, "/md5/") and contains(@class, "js-vim-focus")]'
            )
            if title_links:
                title = ''.join(title_links[0].itertext()).strip()
        except Exception as e:
            logger.debug(f"Title strategy 1 failed: {e}")

        # Estrategia 2: Buscar en data-content
        if not title or len(title) < MIN_TITLE_LENGTH:
            try:
                data_titles = result_div.xpath('.//div[@data-content]/@data-content')
                if data_titles:
                    title = data_titles[0].strip()
            except Exception as e:
                logger.debug(f"Title strategy 2 failed: {e}")

        # Estrategia 3: Buscar cualquier enlace a /md5/
        if not title or len(title) < MIN_TITLE_LENGTH:
            try:
                any_md5_links = result_div.xpath('.//a[starts-with(@href, "/md5/")]')
                if any_md5_links:
                    title = ''.join(any_md5_links[0].itertext()).strip()
            except Exception as e:
                logger.debug(f"Title strategy 3 failed: {e}")

        return title if title and len(title) >= MIN_TITLE_LENGTH else None

    def _extract_author(self, result_div, title: str) -> str:
        """
        Extrae el autor usando múltiples estrategias.

        Args:
            result_div: Elemento HTML del resultado
            title: Título del libro (para evitar duplicación)

        Returns:
            Nombre del autor o string vacío
        """
        try:
            # Estrategia 1: Buscar por icono
            author_links = result_div.xpath(
                './/a[.//span[contains(@class, "icon-[mdi--user-edit]")]]'
            )
            if author_links:
                return ''.join(author_links[0].itertext()).strip()

            # Estrategia 2: Buscar en data-content
            data_authors = result_div.xpath('.//div[@data-content]/@data-content')
            if len(data_authors) > 1:
                return data_authors[1].strip()
            elif data_authors:
                author_candidate = data_authors[0].strip()
                if author_candidate != title:
                    return author_candidate
        except Exception as e:
            logger.debug(f"Error extracting author: {e}")

        return ''

    def _extract_format(self, result_div) -> Optional[str]:
        """
        Extrae el formato del archivo del texto del resultado.

        Args:
            result_div: Elemento HTML del resultado

        Returns:
            Formato del archivo o None
        """
        try:
            all_text = ''.join(result_div.xpath('.//text()')).upper()
            for fmt in COMMON_FORMATS:
                if fmt in all_text:
                    return fmt
        except Exception as e:
            logger.debug(f"Error extracting format: {e}")
        return None

    def _extract_cover_url(self, result_div) -> Optional[str]:
        """
        Extrae la URL de la portada.

        Args:
            result_div: Elemento HTML del resultado

        Returns:
            URL de la portada o None
        """
        try:
            img = result_div.xpath('.//img/@src')
            if img:
                return img[0]
        except Exception as e:
            logger.debug(f"Error extracting cover URL: {e}")
        return None

    def _parse_search_result(self, result_div, seen_md5: set) -> Optional[SearchResult]:
        """
        Parsea un div de resultado en un SearchResult.

        Args:
            result_div: Elemento HTML del resultado
            seen_md5: Set de MD5s ya procesados

        Returns:
            SearchResult o None si el resultado no es válido
        """
        # Extraer MD5
        md5 = self._extract_md5(result_div)
        if not md5 or md5 in seen_md5:
            return None

        seen_md5.add(md5)

        # Extraer título
        title = self._extract_title(result_div)
        if not title:
            return None

        # Crear SearchResult
        s = SearchResult()
        s.detail_item = md5
        s.title = title
        s.author = self._extract_author(result_div, title)
        s.formats = self._extract_format(result_div)
        s.cover_url = self._extract_cover_url(result_div)
        s.price = '$0.00'
        s.drm = SearchResult.DRM_UNLOCKED

        return s

    def _search(self, query: str, max_results: int, timeout: int) -> SearchResults:
        """
        Busca en Anna's Archive usando la nueva estructura HTML.
        Soporta paginación para obtener más resultados.

        Args:
            query: Término de búsqueda
            max_results: Número máximo de resultados a retornar
            timeout: Timeout para las peticiones HTTP

        Yields:
            SearchResult: Resultados de búsqueda uno por uno

        Raises:
            MirrorError: Si no hay mirrors disponibles
        """
        if not query or not query.strip():
            logger.warning("Empty search query provided")
            return

        logger.info(f"Starting search for: '{query}' (max_results={max_results})")

        # Configurar browser
        br = browser()
        br.addheaders = [('User-Agent', USER_AGENT)]

        # Seleccionar mirror funcional
        mirrors = self.config.get('mirrors', DEFAULT_MIRRORS).copy()
        try:
            selected_mirror = self._select_working_mirror(mirrors)
        except MirrorError as e:
            logger.error(str(e))
            raise

        # Construir URL base
        url_base = self._build_search_url(query, selected_mirror)
        logger.debug(f"Search URL base: {url_base}")

        # Inicializar variables de búsqueda
        seen_md5 = set()
        results_count = 0
        page = 1
        max_pages = self.config.get('max_pages', MAX_PAGES_DEFAULT)

        # Iterar por páginas
        while results_count < max_results and page <= max_pages:
            url = f'{url_base}&page={page}'
            logger.debug(f"Fetching page {page}: {url}")

            try:
                with closing(br.open(url, timeout=timeout)) as resp:
                    if resp.code != 200:
                        logger.warning(f"Unexpected status code {resp.code} for page {page}")
                        break

                    try:
                        doc = html.fromstring(resp.read())
                    except ParserError as e:
                        logger.error(f"HTML parse error on page {page}: {e}")
                        break

            except (HTTPError, URLError, TimeoutError) as e:
                logger.warning(f"Network error on page {page}: {e}")
                page += 1
                continue
            except Exception as e:
                logger.error(f"Unexpected error fetching page {page}: {e}")
                page += 1
                continue

            # Parsear resultados de esta página
            try:
                result_divs = doc.xpath(
                    '//div[contains(@class, "flex") and '
                    'contains(@class, "pt-3") and '
                    'contains(@class, "pb-3") and '
                    'contains(@class, "border-b")]'
                )
            except Exception as e:
                logger.error(f"XPath error on page {page}: {e}")
                break

            if not result_divs:
                logger.debug(f"No results found on page {page}")
                break

            logger.debug(f"Found {len(result_divs)} result divs on page {page}")

            # Contador de resultados procesados en esta página
            page_results = 0

            for result_div in result_divs:
                if results_count >= max_results:
                    break

                result = self._parse_search_result(result_div, seen_md5)
                if result:
                    yield result
                    results_count += 1
                    page_results += 1
                    logger.debug(f"Yielded result {results_count}: {result.title}")

            # Si no se procesó ningún resultado en esta página, salir
            if page_results == 0:
                logger.debug(f"No valid results on page {page}, stopping")
                break

            logger.info(f"Page {page}: {page_results} results ({results_count} total)")
            page += 1

        logger.info(f"Search completed: {results_count} results yielded")

    def search(self, query: str, max_results: int = 10, timeout: int = 60) -> SearchResults:
        """
        Búsqueda principal que delega a _search.

        Args:
            query: Término de búsqueda
            max_results: Número máximo de resultados (default: 10)
            timeout: Timeout en segundos (default: 60)

        Yields:
            SearchResult: Resultados de búsqueda
        """
        try:
            yield from self._search(query, max_results, timeout)
        except MirrorError:
            logger.error("No working mirrors available")
            # No yield nada, retorna generador vacío
        except Exception as e:
            logger.exception(f"Unexpected error in search: {e}")

    def open(self, parent=None, detail_item: Optional[str] = None, external: bool = False):
        """
        Abre la página del libro en Anna's Archive.

        Args:
            parent: Widget padre
            detail_item: MD5 del libro
            external: Si True, abre en navegador externo
        """
        if detail_item:
            url = self._get_url(detail_item)
        else:
            url = self.working_mirror or self.config.get('mirrors', DEFAULT_MIRRORS)[0]

        if external or self.config.get('open_external', False):
            open_url(QUrl(url))
        else:
            d = WebStoreDialog(self.gui, self.working_mirror, parent, url)
            d.setWindowTitle(self.name)
            d.set_tags(self.config.get('tags', ''))
            d.exec()

    def get_details(self, search_result: SearchResult, timeout: int = 60):
        """
        Obtiene enlaces de descarga desde la página de detalles.

        Args:
            search_result: Resultado de búsqueda
            timeout: Timeout en segundos
        """
        if not search_result.detail_item:
            logger.warning("No detail_item provided for get_details")
            return

        logger.info(f"Getting details for: {search_result.title}")

        link_opts = self.config.get('link', {})
        url_extension = link_opts.get('url_extension', True)
        content_type = link_opts.get('content_type', False)

        br = browser()
        br.addheaders = [('User-Agent', USER_AGENT)]

        try:
            with closing(br.open(self._get_url(search_result.detail_item), timeout=timeout)) as f:
                doc = html.fromstring(f.read())
        except Exception as e:
            logger.error(f"Error fetching details page: {e}")
            return

        # Buscar enlaces de descarga
        download_selectors = [
            '//div[@id="md5-panel-downloads"]//a[contains(@class, "js-download-link")]',
            '//a[contains(@href, "download") or contains(@href, "libgen") or contains(@href, "zlibrary")]',
            '//div[contains(@class, "download")]//a[@href]'
        ]

        links = []
        for selector in download_selectors:
            try:
                links = doc.xpath(selector)
                if links:
                    logger.debug(f"Found {len(links)} download links with selector: {selector}")
                    break
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")

        if not links:
            logger.warning(f"No download links found for {search_result.title}")
            return

        for link in links:
            url = link.get('href')
            if not url:
                continue

            link_text = ''.join(link.itertext()).strip()
            original_url = url

            # Procesar diferentes fuentes de descarga
            try:
                url = self._process_download_url(url, link_text, br)
            except Exception as e:
                logger.warning(f"Error processing download URL {original_url}: {e}")
                continue

            if not url or not url.startswith('http'):
                continue

            # Validaciones opcionales
            if not self._validate_download_url(url, search_result, content_type, url_extension, timeout):
                continue

            # Agregar enlace de descarga
            label = f"{link_text}.{search_result.formats}" if search_result.formats else link_text
            search_result.downloads[label] = url
            logger.debug(f"Added download link: {label}")

        logger.info(f"Found {len(search_result.downloads)} download links")

    def _process_download_url(self, url: str, link_text: str, br) -> str:
        """
        Procesa URLs de descarga de diferentes fuentes.

        Args:
            url: URL a procesar
            link_text: Texto del enlace
            br: Browser object

        Returns:
            URL procesada
        """
        url_lower = url.lower()

        if 'libgen.li' in url_lower or link_text == 'Libgen.li':
            return self._get_libgen_link(url, br)
        elif 'libgen.rs' in url_lower or 'Libgen.rs' in link_text:
            return self._get_libgen_nonfiction_link(url, br)
        elif 'sci-hub' in url_lower or link_text.startswith('Sci-Hub'):
            return self._get_scihub_link(url, br)
        elif 'z-lib' in url_lower or 'zlibrary' in url_lower or link_text == 'Z-Library':
            return self._get_zlib_link(url, br)
        elif not url.startswith('http'):
            return urljoin(self.working_mirror, url)

        return url

    def _validate_download_url(
        self,
        url: str,
        search_result: SearchResult,
        content_type: bool,
        url_extension: bool,
        timeout: int
    ) -> bool:
        """
        Valida una URL de descarga.

        Args:
            url: URL a validar
            search_result: Resultado de búsqueda
            content_type: Si validar Content-Type
            url_extension: Si validar extensión
            timeout: Timeout en segundos

        Returns:
            True si la URL es válida
        """
        if content_type:
            try:
                with urlopen(Request(url, method='HEAD'), timeout=timeout) as resp:
                    if resp.info().get_content_maintype() != 'application':
                        logger.debug(f"Invalid content type for {url}")
                        return False
            except (HTTPError, URLError, TimeoutError, RemoteDisconnected) as e:
                logger.debug(f"Content-Type validation failed for {url}: {e}")
                # No rechazamos el enlace si falla la validación

        if url_extension and search_result.formats:
            _format = '.' + search_result.formats.lower()
            params = url.find("?")
            end_pos = params if params >= 0 else None
            if not url.endswith(_format, 0, end_pos):
                logger.debug(f"Invalid extension for {url} (expected {_format})")
                return False

        return True

    @staticmethod
    def _get_libgen_link(url: str, br) -> str:
        """
        Obtiene el enlace directo de Libgen.li.

        Args:
            url: URL de la página de Libgen.li
            br: Browser object

        Returns:
            URL de descarga directa o string vacío
        """
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                scheme, _, host, _ = resp.geturl().split('/', 3)

            download_url = ''.join(doc.xpath('//a[h2[text()="GET"]]/@href'))
            if download_url:
                return f"{scheme}//{host}/{download_url}"
        except Exception as e:
            logger.debug(f"Error getting Libgen.li link: {e}")

        return ''

    @staticmethod
    def _get_libgen_nonfiction_link(url: str, br) -> str:
        """
        Obtiene el enlace directo de Libgen.rs.

        Args:
            url: URL de la página de Libgen.rs
            br: Browser object

        Returns:
            URL de descarga directa o string vacío
        """
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())

            download_url = ''.join(doc.xpath('//h2/a[text()="GET"]/@href'))
            return download_url
        except Exception as e:
            logger.debug(f"Error getting Libgen.rs link: {e}")

        return ''

    @staticmethod
    def _get_scihub_link(url: str, br) -> str:
        """
        Obtiene el enlace directo de Sci-Hub.

        Args:
            url: URL de la página de Sci-Hub
            br: Browser object

        Returns:
            URL de descarga directa o string vacío
        """
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                scheme, _ = resp.geturl().split('/', 1)

            pdf_url = ''.join(doc.xpath('//embed[@id="pdf"]/@src'))
            if pdf_url:
                return scheme + pdf_url
        except Exception as e:
            logger.debug(f"Error getting Sci-Hub link: {e}")

        return ''

    @staticmethod
    def _get_zlib_link(url: str, br) -> str:
        """
        Obtiene el enlace directo de Z-Library.

        Args:
            url: URL de la página de Z-Library
            br: Browser object

        Returns:
            URL de descarga directa o string vacío
        """
        try:
            with closing(br.open(url)) as resp:
                doc = html.fromstring(resp.read())
                scheme, _, host, _ = resp.geturl().split('/', 3)

            download_url = ''.join(doc.xpath('//a[contains(@class, "addDownloadedBook")]/@href'))
            if download_url:
                return f"{scheme}//{host}/{download_url}"
        except Exception as e:
            logger.debug(f"Error getting Z-Library link: {e}")

        return ''

    def _get_url(self, md5: str) -> str:
        """
        Construye la URL completa para un MD5.

        Args:
            md5: Hash MD5 del libro

        Returns:
            URL completa
        """
        base = self.working_mirror or self.config.get('mirrors', DEFAULT_MIRRORS)[0]
        return f"{base}/md5/{md5}"

    def config_widget(self):
        """Retorna el widget de configuración del plugin."""
        from calibre_plugins.store_annas_archive.config import ConfigWidget
        return ConfigWidget(self)

    def save_settings(self, config_widget):
        """Guarda la configuración del plugin."""
        config_widget.save_settings()

"""
Configuration widget for the Anna's Archive Calibre plugin.

New in this version:
  • "Test mirrors" button — runs real HTTP health-checks in a background thread
    and displays per-mirror status (✓/✗ + latency) without blocking the UI.
  • max_pages QSpinBox — controls how many result pages the plugin may fetch.
  • timeout QSpinBox  — controls the HTTP request timeout in seconds.
  • Advanced options group that persists both new settings to the store config.
"""

import time
from typing import Dict

from calibre_plugins.store_annas_archive.constants import (
    DEFAULT_MIRRORS, DEFAULT_TIMEOUT, MAX_PAGES_DEFAULT,
    SearchConfiguration, Order, Content, Access, FileType, Source, Language,
)

try:
    from qt.core import (
        Qt,
        QWidget, QGridLayout, QVBoxLayout, QHBoxLayout, QLabel,
        QFrame, QGroupBox, QScrollArea, QAbstractScrollArea,
        QComboBox, QCheckBox, QSizePolicy,
        QListWidget, QListWidgetItem, QAbstractItemView,
        QShortcut, QKeySequence,
        QSpinBox, QPushButton, QThread, pyqtSignal,
    )
except (ImportError, ModuleNotFoundError):
    from PyQt5.QtCore import Qt, QThread
    from PyQt5.QtCore import pyqtSignal
    from PyQt5.QtWidgets import (
        QWidget, QGridLayout, QVBoxLayout, QHBoxLayout, QLabel,
        QFrame, QGroupBox, QScrollArea, QAbstractScrollArea,
        QComboBox, QCheckBox, QSizePolicy,
        QListWidget, QListWidgetItem, QAbstractItemView,
        QShortcut, QSpinBox, QPushButton,
    )
    from PyQt5.QtGui import QKeySequence

load_translations()


# ---------------------------------------------------------------------------
# Background worker: health-check each mirror and emit per-mirror results
# ---------------------------------------------------------------------------
class MirrorTestWorker(QThread):
    """
    Runs HTTP HEAD requests on each mirror in a background thread.

    Signals:
        mirror_tested(url, ok, latency_ms): emitted after each mirror is tested.
        all_done():                          emitted when every mirror has been tested.
    """

    mirror_tested = pyqtSignal(str, bool, int)   # url, ok, latency_ms
    all_done = pyqtSignal()

    def __init__(self, mirrors: list, timeout: int = 5, parent=None):
        super().__init__(parent)
        self.mirrors = mirrors
        self.timeout = timeout

    def run(self):
        from urllib.request import urlopen, Request
        USER_AGENT = 'Mozilla/5.0 (compatible; CalibrePlugin)'

        for mirror in self.mirrors:
            t0 = time.time()
            ok = False
            try:
                req = Request(mirror + '/', headers={'User-Agent': USER_AGENT}, method='HEAD')
                with urlopen(req, timeout=self.timeout) as resp:
                    ok = resp.code < 500
            except Exception:
                ok = False
            latency = int((time.time() - t0) * 1000)
            self.mirror_tested.emit(mirror, ok, latency)

        self.all_done.emit()


# ---------------------------------------------------------------------------
# Editable, drag-and-drop mirror list
# ---------------------------------------------------------------------------
class MirrorsList(QListWidget):
    def __init__(self, parent=...):
        super().__init__(parent)
        self.setDragEnabled(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)

        self._check_last_changed = False
        self.itemChanged.connect(self.add_mirror)

        self.delete_pressed = QShortcut(QKeySequence(Qt.Key.Key_Delete), self)
        self.delete_pressed.activated.connect(self.delete_item)

    def delete_item(self):
        if self.currentRow() != self.count() - 1:
            self.takeItem(self.currentRow())

    def load_mirrors(self, mirrors):
        self._check_last_changed = False
        for mirror in mirrors:
            item = QListWidgetItem(mirror, self)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsEditable)
        self._add_last_list_item()
        self._check_last_changed = True

    def _add_last_list_item(self):
        item = QListWidgetItem('', self)
        item.setFlags(
            Qt.ItemFlag.ItemIsSelectable |
            Qt.ItemFlag.ItemIsEditable |
            Qt.ItemFlag.ItemIsEnabled
        )

    def dropEvent(self, event):
        y = event.pos().y()
        if (self.count() < 5 and y <= (self.count() * 16) - 10) or \
           (self.count() >= 5 and y <= 70):
            return super().dropEvent(event)

    def add_mirror(self, item):
        if self._check_last_changed and self.count() == self.indexFromItem(item).row() + 1:
            if item.text():
                self._check_last_changed = False
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsDragEnabled)
                self._add_last_list_item()
                self._check_last_changed = True

    def get_mirrors(self) -> list:
        return [
            item
            for i in range(self.count())
            if (item := str(self.item(i).text()))
        ]


# ---------------------------------------------------------------------------
# Main configuration widget
# ---------------------------------------------------------------------------
class ConfigWidget(QWidget):
    def __init__(self, store):
        super().__init__()
        self.store = store
        self.resize(680, 820)

        self._test_worker: MirrorTestWorker | None = None
        self._test_results: dict = {}
        self._test_order: list = []

        main_layout = QVBoxLayout(self)

        # ── Search options ─────────────────────────────────────────────
        search_options = QGroupBox(_('Search options'), self)
        search_options.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        search_grid = QGridLayout(search_options)
        search_grid.setContentsMargins(3, 3, 3, 3)

        ordering_label = QLabel(_('Ordering:'), search_options)
        ordering_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        search_grid.addWidget(ordering_label, 0, 0)
        order = QComboBox(search_options)
        for txt, value in Order.options:
            order.addItem(txt, value)
        order.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        search_grid.addWidget(order, 0, 1)
        self.order = Order(order)

        self.search_options: Dict[str, SearchConfiguration] = {
            self.order.config_option: self.order
        }

        search_grid.addWidget(self._make_cbx_group(search_options, Content()), 1, 0)
        search_grid.addWidget(self._make_cbx_group(search_options, FileType()), 2, 0)
        search_grid.addWidget(self._make_cbx_group(search_options, Access()), 1, 1)
        search_grid.addWidget(self._make_cbx_group(search_options, Source()), 2, 1)
        search_grid.addWidget(
            self._make_cbx_group(search_options, Language(), scrollbar=True), 1, 2, 2, 1
        )
        main_layout.addWidget(search_options)

        # ── Download link options + mirrors ────────────────────────────
        horizontal_layout = QHBoxLayout()

        link_options = QGroupBox(_('Download link options'), self)
        link_options.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        link_layout = QVBoxLayout(link_options)
        link_layout.setContentsMargins(6, 6, 6, 6)

        self.url_extension = QCheckBox(_('Verify url extension'), link_options)
        self.url_extension.setToolTip(
            _('Verify that each download url ends with the correct extension for its format')
        )
        link_layout.addWidget(self.url_extension)

        self.content_type = QCheckBox(_('Verify Content-Type'), link_options)
        self.content_type.setToolTip(
            _("Fetch the header of each site and verify it has an 'application' content type")
        )
        link_layout.addWidget(self.content_type)
        horizontal_layout.addWidget(link_options)

        # Mirrors group
        mirrors_group = QGroupBox(_('Mirrors'), self)
        mirrors_layout = QVBoxLayout(mirrors_group)
        mirrors_layout.setContentsMargins(4, 4, 4, 4)

        self.mirrors = MirrorsList(mirrors_group)
        mirrors_layout.addWidget(self.mirrors)

        test_btn = QPushButton(_('Test mirrors'), mirrors_group)
        test_btn.setToolTip(
            _('Check reachability of each mirror via HTTP HEAD request')
        )
        test_btn.clicked.connect(self._test_mirrors)
        mirrors_layout.addWidget(test_btn)

        self.mirror_status_label = QLabel('', mirrors_group)
        self.mirror_status_label.setWordWrap(True)
        self.mirror_status_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
        )
        self.mirror_status_label.hide()
        mirrors_layout.addWidget(self.mirror_status_label)

        horizontal_layout.addWidget(mirrors_group)
        main_layout.addLayout(horizontal_layout)

        # ── Advanced options ───────────────────────────────────────────
        advanced_group = QGroupBox(_('Advanced options'), self)
        advanced_grid = QGridLayout(advanced_group)
        advanced_grid.setContentsMargins(6, 6, 6, 6)

        # Max pages
        max_pages_label = QLabel(_('Max result pages:'), advanced_group)
        max_pages_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        advanced_grid.addWidget(max_pages_label, 0, 0)

        self.max_pages_spin = QSpinBox(advanced_group)
        self.max_pages_spin.setRange(1, 100)
        self.max_pages_spin.setValue(MAX_PAGES_DEFAULT)
        self.max_pages_spin.setToolTip(
            _('Maximum number of result pages to fetch per search (1 page ≈ 100 results)')
        )
        advanced_grid.addWidget(self.max_pages_spin, 0, 1)

        advanced_grid.addWidget(QLabel('', advanced_group), 0, 2)   # spacer

        # Timeout
        timeout_label = QLabel(_('Request timeout (s):'), advanced_group)
        timeout_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        advanced_grid.addWidget(timeout_label, 0, 3)

        self.timeout_spin = QSpinBox(advanced_group)
        self.timeout_spin.setRange(5, 300)
        self.timeout_spin.setValue(DEFAULT_TIMEOUT)
        self.timeout_spin.setToolTip(
            _('Seconds to wait for each HTTP request before giving up')
        )
        advanced_grid.addWidget(self.timeout_spin, 0, 4)

        advanced_grid.setColumnStretch(2, 1)
        main_layout.addWidget(advanced_group)

        # ── Misc ───────────────────────────────────────────────────────
        self.open_external = QCheckBox(_('Open store in external web browser'), self)
        main_layout.addWidget(self.open_external)

        self.load_settings()

    # ------------------------------------------------------------------
    # Mirror testing
    # ------------------------------------------------------------------

    def _test_mirrors(self):
        mirrors = self.mirrors.get_mirrors()
        if not mirrors:
            self.mirror_status_label.setText(_('No mirrors configured.'))
            self.mirror_status_label.show()
            return

        # Cancel any previous worker
        if self._test_worker and self._test_worker.isRunning():
            self._test_worker.terminate()
            self._test_worker.wait()

        self._test_results = {}
        self._test_order = mirrors[:]
        self.mirror_status_label.setText(_('Testing mirrors…'))
        self.mirror_status_label.show()

        self._test_worker = MirrorTestWorker(
            mirrors,
            timeout=min(self.timeout_spin.value(), 10),
            parent=self,
        )
        self._test_worker.mirror_tested.connect(self._on_mirror_tested)
        self._test_worker.all_done.connect(self._on_mirrors_test_done)
        self._test_worker.start()

    def _on_mirror_tested(self, url: str, ok: bool, latency_ms: int):
        self._test_results[url] = (ok, latency_ms)
        self._refresh_mirror_status()

    def _on_mirrors_test_done(self):
        self._refresh_mirror_status()

    def _refresh_mirror_status(self):
        lines = []
        for mirror in self._test_order:
            if mirror in self._test_results:
                ok, ms = self._test_results[mirror]
                if ok:
                    lines.append(f'✓  {mirror}  ({ms} ms)')
                else:
                    lines.append(f'✗  {mirror}  (unreachable)')
            else:
                lines.append(f'…  {mirror}')
        self.mirror_status_label.setText('\n'.join(lines))

    # ------------------------------------------------------------------
    # Checkbox group builder
    # ------------------------------------------------------------------

    def _make_cbx_group(self, parent, option: SearchConfiguration, scrollbar: bool = False):
        box = QGroupBox(_(option.name), parent)
        vertical_layout = QVBoxLayout(box)
        if scrollbar:
            vertical_layout.setSpacing(0)
            vertical_layout.setContentsMargins(0, 0, 0, 0)

            scroll_area = QScrollArea(box)
            scroll_area.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
            scroll_area.setFrameShape(QFrame.Shape.NoFrame)
            scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
            scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
            scroll_area.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)

            cbx_parent = QWidget()
            cbx_parent.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
            top_vertical = vertical_layout
            vertical_layout = QVBoxLayout(cbx_parent)
        else:
            cbx_parent = box

        vertical_layout.setSpacing(3)
        vertical_layout.setContentsMargins(3, 3, 3, 3)

        for name, type_ in option.options:
            check_box = QCheckBox(cbx_parent)
            check_box.setText(name)
            vertical_layout.addWidget(check_box)
            option.checkboxes[type_] = check_box

        self.search_options[option.config_option] = option

        if scrollbar:
            scroll_area.setWidget(cbx_parent)
            top_vertical.addWidget(scroll_area)

        return box

    # ------------------------------------------------------------------
    # Load / save
    # ------------------------------------------------------------------

    def load_settings(self):
        config = self.store.config

        self.open_external.setChecked(config.get('open_external', False))
        self.mirrors.load_mirrors(config.get('mirrors', DEFAULT_MIRRORS))

        search_opts = config.get('search', {})
        for configuration in self.search_options.values():
            configuration.load(search_opts.get(configuration.config_option, configuration.default))

        link_opts = config.get('link', {})
        self.url_extension.setChecked(link_opts.get('url_extension', True))
        self.content_type.setChecked(link_opts.get('content_type', False))

        # Advanced options
        self.max_pages_spin.setValue(config.get('max_pages', MAX_PAGES_DEFAULT))
        self.timeout_spin.setValue(config.get('timeout', DEFAULT_TIMEOUT))

    def save_settings(self):
        self.store.config['open_external'] = self.open_external.isChecked()
        self.store.config['mirrors'] = self.mirrors.get_mirrors()

        self.store.config['search'] = {
            configuration.config_option: configuration.to_save()
            for configuration in self.search_options.values()
        }
        self.store.config['link'] = {
            'url_extension': self.url_extension.isChecked(),
            'content_type': self.content_type.isChecked(),
        }

        # Advanced options
        self.store.config['max_pages'] = self.max_pages_spin.value()
        self.store.config['timeout'] = self.timeout_spin.value()

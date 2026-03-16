# Calibre plugin — Anna's Archive

Browse and download millions of books, papers, and magazines directly from Calibre via [Anna's Archive](https://annas-archive.gl) — the world's largest shadow library, aggregating Libgen, Z-Library, Sci-Hub, Internet Archive, and more.

<img width="789" height="463" alt="image" src="https://github.com/user-attachments/assets/6efde1a5-c93d-4c8c-8783-efefe4f58b24" />

---

## Installation

1. Go to the [latest release](../../releases/latest) and download `Anna's Archive.zip`
2. In Calibre: **Preferences → Plugins → Load plugin from file**
3. Select the downloaded `.zip` — no extraction needed
4. Restart Calibre

---

## Usage

Open the store via **Store → Search stores** or the store icon in the toolbar. Search by title, author, or keyword. Use the filters in **Configure** to narrow by language, filetype, source, etc.

Click the green arrow on any result to download directly to your library.

---

### Search filters

For checkbox options (filetype, language, source, content, access): if no boxes are checked, the filter is disabled and all results are shown. If any box is checked, only results matching that selection are returned.

---

## Building from source

```bash
git clone https://github.com/a-peirogon/cal-annas.git
cd cal-annas
zip Anna\'s\ Archive.zip __init__.py annas_archive.py config.py constants.py plugin-import-name-store_annas_archive.txt
calibre-customize -a "Anna's Archive.zip"
```

---

## Credits

- Original plugin by [ScottBot10](https://github.com/ScottBot10/calibre_annas_archive)
- [Anna's Archive](https://annas-archive.gl) — the world's largest open-source shadow library

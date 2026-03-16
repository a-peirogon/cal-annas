from calibre.customize import StoreBase


class AnnasArchiveStore(StoreBase):
    name                    = "Anna's Archive"
    description             = "Search and download books from Anna's Archive — indexes Libgen, Z-Library, Sci-Hub, Internet Archive, and more."
    supported_platforms     = ['windows', 'osx', 'linux']
    author                  = 'a-peirogon'
    version                 = (0, 3, 0)
    minimum_calibre_version = (5, 0, 0)
    formats                 = ['EPUB', 'MOBI', 'PDF', 'AZW3', 'CBR', 'CBZ', 'FB2', 'DJVU', 'TXT']
    drm_free_only           = True

    actual_plugin = 'calibre_plugins.store_annas_archive.annas_archive:AnnasArchiveStore'

    def is_customizable(self):
        return True

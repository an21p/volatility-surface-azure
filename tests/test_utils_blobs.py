from datetime import datetime
from unittest.mock import MagicMock

import utils


def test_get_analysis_blob_name():
    name = utils.get_analysis_blob_name("TSLA", datetime(2026, 5, 24))
    assert name == "TSLA_analysis_20260524.txt"


def test_write_then_read_text_blob_roundtrip():
    store = {}
    container = MagicMock()

    def get_blob_client(name):
        bc = MagicMock()
        bc.upload_blob.side_effect = lambda data, overwrite: store.__setitem__(name, data)
        bc.download_blob.return_value.readall.return_value = store.get(name, b"")
        return bc

    container.get_blob_client.side_effect = get_blob_client

    utils.write_text_blob(container, "x.txt", "héllo")
    assert utils.read_text_blob(container, "x.txt") == "héllo"

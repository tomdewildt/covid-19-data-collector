import pytest
import mock

from collector.store import LocalStore


class TestStoreOpen:
    @mock.patch("collector.store.open")
    @mock.patch.object(LocalStore, "_ensure_dir_exists")
    def test_open_write(self, mock_ensure_dir_exists, mock_open):
        store = LocalStore("/tmp/")
        with store.open("test.txt", "w") as handle:
            handle.write("content")

        mock_ensure_dir_exists.assert_called_once_with("/tmp/test.txt")
        mock_open.assert_called_once_with("/tmp/test.txt", "w", encoding="utf8")

    @mock.patch("collector.store.open")
    @mock.patch.object(LocalStore, "_ensure_dir_exists")
    def test_open_read(self, mock_ensure_dir_exists, mock_open):
        mock_open.side_effect = [mock.mock_open(read_data="content").return_value]

        store = LocalStore("/tmp/")
        with store.open("test.txt", "r") as handle:
            content = handle.readlines()

        mock_ensure_dir_exists.assert_not_called()
        mock_open.assert_called_once_with("/tmp/test.txt", "r", encoding="utf8")

        assert content == ["content"]


class TestStoreList:
    @mock.patch("glob.glob")
    def test_list(self, mock_glob):
        store = LocalStore("/tmp")
        store.list("*.txt")

        mock_glob.assert_called_once_with("/tmp/*.txt")


class TestStoreIsWriteMode:
    @pytest.mark.parametrize(
        "mode,expected",
        [
            ("r", False),
            ("rb", False),
            ("r+", False),
            ("rb+", False),
            ("w", True),
            ("wb", True),
            ("w+", True),
            ("wb+", True),
            ("a", False),
            ("ab", False),
            ("a+", False),
            ("ab+", False),
        ],
    )
    def test_is_write_mode(self, mode, expected):
        # pylint: disable=protected-access
        assert LocalStore("/tmp")._is_write_mode(mode) == expected


class TestStoreEnsureDirExists:
    @mock.patch("os.makedirs")
    def test_ensure_dir_exists(self, mock_makedirs):
        # pylint: disable=protected-access
        LocalStore("/tmp")._ensure_dir_exists("/tmp/test.txt")

        mock_makedirs.assert_called_once_with("/tmp")

    @mock.patch("os.makedirs")
    def test_ensure_dir_exists_error(self, mock_makedirs):
        mock_makedirs.side_effect = OSError

        # pylint: disable=protected-access
        LocalStore("/tmp")._ensure_dir_exists("/tmp/test.txt")

        mock_makedirs.assert_called_once_with("/tmp")

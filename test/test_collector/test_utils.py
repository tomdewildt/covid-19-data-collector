import pytest

from collector.utils import filter_files


class TestFilterFiles:
    @pytest.mark.parametrize(
        "files,output",
        [
            ([], []),
            (["/tmp/1970-01-01-file-1.csv"], ["/tmp/1970-01-01-file-1.csv"]),
            (
                [
                    "/tmp/1970-01-01-file-1.csv",
                    "/tmp/1970-01-02-file-1.csv",
                    "/tmp/1970-01-03-file-1.csv",
                    "/tmp/1970-01-04-file-1.csv",
                ],
                ["/tmp/1970-01-04-file-1.csv"],
            ),
            (
                [
                    "/tmp/1969-01-01-file-1.csv",
                    "/tmp/1969-01-01-file-2.csv",
                    "/tmp/1970-01-01-file-1.csv",
                    "/tmp/1970-01-01-file-2.csv",
                ],
                ["/tmp/1970-01-01-file-1.csv", "/tmp/1970-01-01-file-2.csv"],
            ),
        ],
    )
    def test_filter_files(self, files, output):
        assert filter_files(files) == output

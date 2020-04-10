import pytest

from collector.utils import format_date, filter_files


class TestFormatDate:
    @pytest.mark.parametrize(
        "metadata,date",
        [
            ({"nl": {"mapSubtitle": "content 01-01-2020"}}, "2020-01-01"),
            ({"nl": {"mapSubtitle": "content 1-1-2020"}}, "2020-01-01"),
        ],
    )
    def test_format_date(self, metadata, date):
        assert format_date(metadata) == date


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
    def test_format_date(self, files, output):
        assert filter_files(files) == output

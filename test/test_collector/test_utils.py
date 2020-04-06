import pytest

from collector.utils import format_date


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

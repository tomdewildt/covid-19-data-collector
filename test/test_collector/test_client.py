import pytest
import mock

from fixtures import Response

from collector.client import WebClient, WebClientError


class TestWebClientGet:
    @pytest.mark.parametrize(
        "url,params,text,status",
        [
            ("https://example.com", None, "content", 200),
            ("https://example.com", {"param": "value"}, "content", 200),
        ],
    )
    @mock.patch("requests.get")
    def test_get_valid_response(self, mock_get, url, params, text, status):
        mock_get.return_value = Response(text, status)

        client = WebClient()
        res = client.get(url, params)

        mock_get.assert_called_once_with(url, params=params)

        assert res == text

    @pytest.mark.parametrize(
        "url,params,text,status",
        [
            ("https://example.com", None, "Bad Request", 400),
            ("https://example.com", None, "Unauthorized", 401),
            ("https://example.com", None, "Forbidden", 403),
            ("https://example.com", None, "Not Found", 404),
            ("https://example.com", None, "Method Not Allowed", 405),
            ("https://example.com", None, "Internal Server Error", 500),
            ("https://example.com", None, "Bad Gateway", 502),
            ("https://example.com", None, "Service Unavailable", 503),
            ("https://example.com", None, "Gateway Timeout", 504),
        ],
    )
    @mock.patch("requests.get")
    def test_get_invalid_response(self, mock_get, url, params, text, status):
        mock_get.return_value = Response(text, status)

        client = WebClient()
        with pytest.raises(WebClientError) as error:
            client.get(url, params)

        mock_get.assert_called_once_with(url, params=params)

        assert error.value.message == f"Request to {url} failed"
        assert error.value.status_code == status
        assert error.value.text == text

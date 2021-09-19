import requests


class WebClientError(Exception):
    def __init__(self, message, status_code, text):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.text = text


class WebClient:
    def get(self, url, params=None):
        res = requests.get(url, params=params)

        if res.status_code != 200:
            raise WebClientError(f"Request to {url} failed", res.status_code, res.text)

        return res.text

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"

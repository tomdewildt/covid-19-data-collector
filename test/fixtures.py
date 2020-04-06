class Client:
    def get(self, url, params=None):
        pass


class Response:
    def __init__(self, text, status_code):
        self.text = text
        self.status_code = status_code


class Store:
    def open(self, path, mode, *args, **kwargs):
        pass

    def list(self, path):
        pass

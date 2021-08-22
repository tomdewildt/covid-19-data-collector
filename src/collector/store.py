from contextlib import contextmanager
import glob
import os
import re


class LocalStore:
    def __init__(self, base_path):
        self._base_path = base_path

    @contextmanager
    def open(self, path, mode, *args, **kwargs):
        expanded_path = os.path.join(self._base_path, path)
        if self._is_write_mode(mode):
            self._ensure_dir_exists(expanded_path)
        with open(expanded_path, mode, encoding="utf8", *args, **kwargs) as handle:
            yield handle

    def list(self, path):
        expanded_path = os.path.join(self._base_path, path)
        return glob.glob(expanded_path)

    def _is_write_mode(self, mode):
        return bool(re.match(r"w[+a-z]*", mode))

    def _ensure_dir_exists(self, path):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass

    def __repr__(self):
        return "<{}()>".format(self.__class__.__name__)

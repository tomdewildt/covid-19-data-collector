from functools import lru_cache
import logging.config
import logging
import os

import yaml


def load_config(stream):
    return yaml.load(stream, Loader=yaml.SafeLoader)


@lru_cache(maxsize=1)
def read_config(path=None):
    path = path or os.environ.get("CONFIG", "config.yaml")

    with open(path, "r", encoding="utf8") as stream:
        return load_config(stream)


def init_logging(config=None):
    config = config or read_config()
    path = config.get("log_config")

    if path is None:
        return

    with open(path, "r", encoding="utf8") as handle:
        log_config = yaml.load(handle.read(), Loader=yaml.SafeLoader)
        logging.config.dictConfig(log_config)

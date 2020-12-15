import json
import csv
import io

_CONFIG_DEFAULTS = {
    "environment": "test",
    "log_config": None,
    "collector": {
        "urls": {
            "national": "http://data.com/national",
            "municipality": "http://data.com/municipality",
            "intensive_care": [
                "http://data.com/ic-count",
                "http://data.com/new-intake",
                "http://data.com/died-and-survivors-cumulative",
            ],
        },
        "municipalities": "external/gemeenten.csv",
    },
    "store": {"path": "/tmp"},
}

_LOG_CONFIG_DEFAULTS = {
    "version": 1,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {"level": "DEBUG", "handlers": ["console"],},
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)-8s %(name)-15s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "disable_existing_loggers": False,
}

_RESPONSE_NATIONAL_DEFAULTS = [
    {
        "Date_of_report": "1970-01-01 12:00:00",
        "Municipality_code": "GM0001",
        "Municipality_name": "gemeente 1",
        "Province": "provincie 1",
        "Total_reported": 500,
        "Hospital_admission": 1000,
        "Deceased": 1500,
    },
    {
        "Date_of_report": "1970-01-01 12:00:00",
        "Municipality_code": "GM0002",
        "Municipality_name": "gemeente 2",
        "Province": "provincie 2",
        "Total_reported": 500,
        "Hospital_admission": 1000,
        "Deceased": 1500,
    },
]

_RESPONSE_MUNICIPALITY_DEFAULTS = [
    {
        "Date_of_report": "1970-01-01 12:00:00",
        "Municipality_code": "GM0001",
        "Municipality_name": "gemeente 1",
        "Province": "provincie 1",
        "Total_reported": 500,
        "Hospital_admission": 1000,
        "Deceased": 1500,
    },
    {
        "Date_of_report": "1970-01-01 12:00:00",
        "Municipality_code": "GM0002",
        "Municipality_name": "gemeente 2",
        "Province": "provincie 2",
        "Total_reported": 500,
        "Hospital_admission": 1000,
        "Deceased": 1500,
    },
]

_RESPONSE_INTENSIVE_CARE_DEFAULTS = [
    [{"date": "1970-01-01", "value": 100}],
    [[{"date": "1970-01-01", "value": 100}], [{"date": "1970-01-01", "value": 100}]],
    [[{"date": "1970-01-01", "value": 100}], [{"date": "1970-01-01", "value": 100}]],
]


def create_config(**kwargs):
    return {name: kwargs.get(name, value) for (name, value) in _CONFIG_DEFAULTS.items()}


def create_log_config(**kwargs):
    return {
        name: kwargs.get(name, value) for (name, value) in _LOG_CONFIG_DEFAULTS.items()
    }


def create_national_response():
    buffer = io.StringIO()
    keys = _RESPONSE_NATIONAL_DEFAULTS[0].keys()

    writer = csv.DictWriter(buffer, keys, delimiter=";")
    writer.writeheader()
    writer.writerows(_RESPONSE_NATIONAL_DEFAULTS)

    return buffer.getvalue()


def create_municipality_response():
    buffer = io.StringIO()
    keys = _RESPONSE_MUNICIPALITY_DEFAULTS[0].keys()

    writer = csv.DictWriter(buffer, keys, delimiter=";")
    writer.writeheader()
    writer.writerows(_RESPONSE_MUNICIPALITY_DEFAULTS)

    return buffer.getvalue()


def create_intensive_care_response():
    return list(map(json.dumps, _RESPONSE_INTENSIVE_CARE_DEFAULTS))

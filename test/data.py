import string

_CONFIG_DEFAULTS = {
    "environment": "test",
    "log_config": None,
    "collector": {
        "urls": {
            "national": "http://data.com/national",
            "municipality": "http://data.com/municipality",
        },
        "municipalities": "external/gemeenten.csv",
        "elements": {
            "national": "table",
            "municipality": "data",
            "metadata": "metadata",
        },
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

_REPONSE_DEFAULTS = """
    <table>
        <tbody>
            <tr>
                <td />
                <td><h4>$tested_positive</h4></td>
            <tr>
                <td />
			    <td><h4>$hospitalized</h4></td>
		    </tr>
            <tr>
                <td />
			    <td><h4>$passed_away</h4></td>
            </tr>
        </tbody>
    </table>
    <div id="data">$data</div>
    <div id="metadata">{ "nl":  { "mapSubtitle":"$metadata" } }</div>
"""


def create_config(**kwargs):
    return {name: kwargs.get(name, value) for (name, value) in _CONFIG_DEFAULTS.items()}


def create_log_config(**kwargs):
    return {
        name: kwargs.get(name, value) for (name, value) in _LOG_CONFIG_DEFAULTS.items()
    }


def create_response(**kwargs):
    response = string.Template(_REPONSE_DEFAULTS)
    return response.safe_substitute(**kwargs)

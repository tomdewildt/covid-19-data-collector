import os
import io

import yaml
import mock

from collector.config import load_config, read_config, init_logging
from data import create_config, create_log_config


class TestLoadConfig:
    @property
    def config(self):
        return create_config()

    def test_load_config(self):
        buffer = io.StringIO(yaml.dump(self.config))
        config = load_config(buffer)

        assert config == self.config


class TestReadConfig:
    @property
    def config(self):
        return create_config()

    @mock.patch("collector.config.open")
    def test_read_config_with_path(self, mock_open):
        mock_open.side_effect = [
            mock.mock_open(read_data=yaml.dump(self.config)).return_value
        ]

        config = read_config("env/test/config.yaml")

        mock_open.assert_called_once_with("env/test/config.yaml", "r")

        assert config == self.config

    @mock.patch("collector.config.open")
    @mock.patch.dict(os.environ, {"CONFIG": "env/test/config.yaml"})
    def test_read_config_without_path(self, mock_open):
        mock_open.side_effect = [
            mock.mock_open(read_data=yaml.dump(self.config)).return_value
        ]

        config = read_config()

        mock_open.assert_called_once_with("env/test/config.yaml", "r")

        assert config == self.config


class TestInitLogging:
    @property
    def config(self):
        return create_config(log_config="env/test/logging.yaml")

    @property
    def log_config(self):
        return create_log_config()

    @mock.patch("collector.config.open")
    @mock.patch("collector.config.read_config")
    def test_with_config(self, mock_read_config, mock_open):
        mock_read_config.return_value = self.config
        mock_open.side_effect = [
            mock.mock_open(read_data=yaml.dump(self.log_config)).return_value
        ]

        init_logging(self.config)

        mock_read_config.assert_not_called()
        mock_open.assert_called_once_with("env/test/logging.yaml", "r")

    @mock.patch("collector.config.open")
    @mock.patch("collector.config.read_config")
    def test_without_log_config_path(self, mock_read_config, mock_open):
        mock_read_config.return_value = {**self.config, "log_config": None}
        mock_open.side_effect = [
            mock.mock_open(read_data=yaml.dump(self.log_config)).return_value
        ]

        init_logging()

        mock_read_config.assert_called_once()
        mock_open.assert_not_called()

import json

import pytest
import mock

from fixtures import Client, Store
from data import create_config, create_intensive_care_response

from collector.tasks.get_intensive_care_dataset.task import GetIntensiveCareDataset
from collector.schema import ValidationError


class TestGetIntensiveCareDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(GetIntensiveCareDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = GetIntensiveCareDataset(self.config["collector"], Client(), Store())
        task(output_folder="raw")

        mock_run.assert_called_once_with({"output_folder": "raw"})

    @pytest.mark.parametrize(
        "inputs,messages",
        [
            ({}, ["'output_folder' is a required property"]),
            ({"output_folder": 1}, ["1 is not of type 'string'"]),
            ({"output-folder": 1}, ["'output_folder' is a required property"]),
        ],
    )
    @mock.patch.object(GetIntensiveCareDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = GetIntensiveCareDataset(self.config["collector"], Client(), Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Client, "get")
    @mock.patch.object(GetIntensiveCareDataset, "_write")
    def test_run(self, mock_write, mock_get):
        response = create_intensive_care_response()
        mock_get.side_effect = response

        task = GetIntensiveCareDataset(self.config["collector"], Client(), Store())
        task(output_folder="raw")

        mock_get.assert_has_calls(
            [
                mock.call(self.config["collector"]["urls"]["intensive_care"][0]),
                mock.call(self.config["collector"]["urls"]["intensive_care"][1]),
                mock.call(self.config["collector"]["urls"]["intensive_care"][2]),
            ]
        )
        mock_write.assert_has_calls(
            [
                mock.call(json.loads(response[0]), "raw/1970-01-01-ic-count.json"),
                mock.call(
                    json.loads(response[1])[0],
                    "raw/1970-01-01-new-intake-confirmed.json",
                ),
                mock.call(
                    json.loads(response[1])[1],
                    "raw/1970-01-01-new-intake-suspicious.json",
                ),
                mock.call(
                    json.loads(response[2])[0], "raw/1970-01-01-died-cumulative.json",
                ),
                mock.call(
                    json.loads(response[2])[1],
                    "raw/1970-01-01-survived-cumulative.json",
                ),
            ]
        )


class TestGetIntensiveCareDatasetWrite:
    @mock.patch.object(Store, "open")
    def test_write(self, mock_open):
        # pylint: disable=protected-access
        task = GetIntensiveCareDataset(None, Client(), Store())
        task._write({"key": "value"}, "test.csv")

        mock_open.assert_called_once_with("test.csv", "w")

import pandas as pd
import numpy as np
import pytest
import mock

from collector.tasks.get_general_dataset.task import (
    GetGeneralDataset,
    GetGeneralDatasetError,
)
from collector.schema import ValidationError
from data import create_config, create_response
from fixtures import Client, Store


class TestGetGeneralDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(GetGeneralDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = GetGeneralDataset(self.config["collector"], Client(), Store())
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
    @mock.patch.object(GetGeneralDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = GetGeneralDataset(self.config["collector"], Client(), Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Client, "get")
    def test_run_invalid_data_element(self, mock_get):
        mock_get.return_value = "<div id='metadata'></div>"

        task = GetGeneralDataset(self.config["collector"], Client(), Store())
        with pytest.raises(GetGeneralDatasetError) as error:
            task(output_folder="raw")

        assert str(error.value) == "Data element not found in document"

    @mock.patch.object(Client, "get")
    def test_run_invalid_metadata_element(self, mock_get):
        mock_get.return_value = "<table></table>"

        task = GetGeneralDataset(self.config["collector"], Client(), Store())
        with pytest.raises(GetGeneralDatasetError) as error:
            task(output_folder="raw")

        assert str(error.value) == "Metadata element not found in document"

    @mock.patch.object(Client, "get")
    @mock.patch.object(GetGeneralDataset, "_write")
    def test_run(self, mock_write, mock_get):
        mock_get.return_value = create_response(
            tested_positive="1.000",
            hospitalized="2.000",
            passed_away="3.000*",
            metadata="map subtitle 1-1-1970",
        )

        task = GetGeneralDataset(self.config["collector"], Client(), Store())
        task(output_folder="raw")

        mock_get.assert_called_once_with(self.config["collector"]["url"])
        mock_write.assert_called_once_with(mock.ANY, "raw/1970-01-01.csv", index=False)

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(
                {"PositiefGetest": [1000], "Opgenomen": [2000], "Overleden": [3000]}
            ),
            check_dtype=False,
        )


class TestGetGeneralDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = GetGeneralDataset(None, Client(), Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

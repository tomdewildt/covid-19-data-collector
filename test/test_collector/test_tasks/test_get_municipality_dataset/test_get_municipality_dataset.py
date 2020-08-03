import pandas as pd
import numpy as np
import pytest
import mock

from collector.tasks.get_municipality_dataset.task import (
    GetMunicipalityDataset,
    GetMunicipalityDatasetError,
)
from collector.schema import ValidationError
from data import create_config, create_municipality_response
from fixtures import Client, Store


class TestGetMunicipalityDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(GetMunicipalityDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = GetMunicipalityDataset(self.config["collector"], Client(), Store())
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
    @mock.patch.object(GetMunicipalityDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = GetMunicipalityDataset(self.config["collector"], Client(), Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Client, "get")
    def test_run_invalid_data_element(self, mock_get):
        mock_get.return_value = "<span class='date'></div>"

        task = GetMunicipalityDataset(self.config["collector"], Client(), Store())
        with pytest.raises(GetMunicipalityDatasetError) as error:
            task(output_folder="raw")

        assert str(error.value) == "Data element not found in document"

    @mock.patch.object(Client, "get")
    def test_run_invalid_metadata_element(self, mock_get):
        mock_get.return_value = "<div id='municipality'></div>"

        task = GetMunicipalityDataset(self.config["collector"], Client(), Store())
        with pytest.raises(GetMunicipalityDatasetError) as error:
            task(output_folder="raw")

        assert str(error.value) == "Date element not found in document"

    @mock.patch.object(Client, "get")
    @mock.patch.object(GetMunicipalityDataset, "_write")
    def test_run(self, mock_write, mock_get):
        mock_get.return_value = create_municipality_response(
            municipality="Gemnr;Gemeente;Totaal_Absoluut\n1;gemeente 1;1\n2;gemeente 2;2\n",
            date="date 1-1-1970 | 00:00",
        )

        task = GetMunicipalityDataset(self.config["collector"], Client(), Store())
        task(output_folder="raw")

        mock_get.assert_called_once_with(
            self.config["collector"]["urls"]["municipality"]
        )
        mock_write.assert_called_once_with(mock.ANY, "raw/1970-01-01.csv", index=False)

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(
                {
                    "Gemnr": [1, 2],
                    "Gemeente": ["gemeente 1", "gemeente 2"],
                    "Totaal_Absoluut": [1, 2],
                }
            ),
            check_dtype=False,
        )


class TestGetMunicipalityDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = GetMunicipalityDataset(None, Client(), Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

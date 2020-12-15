import pandas as pd
import numpy as np
import pytest
import mock

from fixtures import Store
from data import create_config

from collector.tasks.clean_municipality_dataset.task import CleanMunicipalityDataset
from collector.schema import ValidationError


class TestCleanMunicipalityDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(CleanMunicipalityDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = CleanMunicipalityDataset(self.config["collector"], Store())
        task(input_folder="raw", output_folder="processed")

        mock_run.assert_called_once_with(
            {"input_folder": "raw", "output_folder": "processed"}
        )

    @pytest.mark.parametrize(
        "inputs,messages",
        [
            (
                {},
                [
                    "'input_folder' is a required property",
                    "'output_folder' is a required property",
                ],
            ),
            (
                {"input_folder": 1, "output_folder": 1},
                ["1 is not of type 'string'", "1 is not of type 'string'",],
            ),
            (
                {"input-folder": 1, "output-folder": 1},
                [
                    "'input_folder' is a required property",
                    "'output_folder' is a required property",
                ],
            ),
        ],
    )
    @mock.patch.object(CleanMunicipalityDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = CleanMunicipalityDataset(self.config["collector"], Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Store, "list")
    @mock.patch.object(CleanMunicipalityDataset, "_read")
    @mock.patch.object(CleanMunicipalityDataset, "_write")
    @pytest.mark.parametrize(
        "input_date,input_dataset,output_dataset",
        [
            (
                "1970-01-01",
                {"id": [1], "Gemeente": ["gemeente 1"], "PositiefGetest": [100]},
                {
                    "Gemeentecode": [1],
                    "PositiefGetest": [100],
                    "Gemeente": ["gemeente 1"],
                    "Provinciecode": [2],
                    "Provincie": ["provincie 2"],
                    "Datum": ["1970-01-01"],
                },
            ),
            (
                "1970-01-01",
                {"Gemnr": [1], "Gemeente": ["gemeente 1"], "PositiefGetest": [100]},
                {
                    "Gemeentecode": [1],
                    "PositiefGetest": [100],
                    "Gemeente": ["gemeente 1"],
                    "Provinciecode": [2],
                    "Provincie": ["provincie 2"],
                    "Datum": ["1970-01-01"],
                },
            ),
            (
                "1970-01-01",
                {"id": [-1], "Gemeente": ["10 onbekend"], "PositiefGetest": [99]},
                {
                    "Gemeentecode": [-1],
                    "PositiefGetest": [10],
                    "Gemeente": [None],
                    "Provinciecode": [-1],
                    "Provincie": [None],
                    "Datum": ["1970-01-01"],
                },
            ),
            (
                "1970-01-01",
                {
                    "id": [-1],
                    "Gemeente": ["9 onbekend 1 buitenland"],
                    "PositiefGetest": [99],
                },
                {
                    "Gemeentecode": [-1],
                    "PositiefGetest": [10],
                    "Gemeente": [None],
                    "Provinciecode": [-1],
                    "Provincie": [None],
                    "Datum": ["1970-01-01"],
                },
            ),
            (
                "1970-01-01",
                {
                    "Municipality_code": ["GM0001"],
                    "Municipality_name": ["gemeente 1"],
                    "Total_reported": [100],
                },
                {
                    "Gemeentecode": [1],
                    "PositiefGetest": [100],
                    "Gemeente": ["gemeente 1"],
                    "Provinciecode": [2],
                    "Provincie": ["provincie 2"],
                    "Datum": ["1970-01-01"],
                },
            ),
            (
                "2020-03-31",
                {"id": [1], "Gemeente": ["gemeente 1"], "PositiefGetest": [100]},
                {
                    "Gemeentecode": [1],
                    "Opgenomen": [100],
                    "Gemeente": ["gemeente 1"],
                    "Provinciecode": [2],
                    "Provincie": ["provincie 2"],
                    "Datum": ["2020-03-31"],
                },
            ),
        ],
    )
    def test_run(
        self,
        mock_write,
        mock_read,
        mock_list,
        input_date,
        input_dataset,
        output_dataset,
    ):
        mock_list.return_value = [f"raw/{input_date}.csv"]
        mock_read.side_effect = [
            pd.DataFrame(
                {
                    "Gemeentecode": [1],
                    "Gemeente": ["gemeente 1"],
                    "Provinciecode": [2],
                    "Provincie": ["provincie 2"],
                }
            ),
            pd.DataFrame(input_dataset),
        ]

        task = CleanMunicipalityDataset(self.config["collector"], Store())
        task(input_folder="raw", output_folder="interim")

        mock_list.assert_called_once_with("raw/*.csv")
        mock_read.assert_has_calls(
            [mock.call("external/gemeenten.csv"), mock.call(f"raw/{input_date}.csv")]
        )
        mock_write.assert_called_once_with(
            mock.ANY, f"interim/{input_date}.csv", index=False
        )

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(output_dataset),
            check_dtype=False,
        )


class TestCleanMunicipalityDatasetRead:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd, "read_csv")
    def test_read(self, mock_read_csv, mock_open):
        # pylint: disable=protected-access
        task = CleanMunicipalityDataset(None, Store())
        task._read("test.csv", delimiter=",")

        mock_read_csv.assert_called_once_with(mock.ANY, delimiter=",")
        mock_open.assert_called_once_with("test.csv", "r")


class TestCleanMunicipalityDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = CleanMunicipalityDataset(None, Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

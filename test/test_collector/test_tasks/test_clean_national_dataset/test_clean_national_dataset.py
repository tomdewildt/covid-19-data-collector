import pandas as pd
import numpy as np
import pytest
import mock

from collector.tasks.clean_national_dataset.task import CleanNationalDataset
from collector.schema import ValidationError
from data import create_config
from fixtures import Store


class TestCleanNationalDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(CleanNationalDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = CleanNationalDataset(self.config["collector"], Store())
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
    @mock.patch.object(CleanNationalDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = CleanNationalDataset(self.config["collector"], Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Store, "list")
    @mock.patch.object(CleanNationalDataset, "_read")
    @mock.patch.object(CleanNationalDataset, "_write")
    def test_run(self, mock_write, mock_read, mock_list):
        mock_list.return_value = ["raw/1970-01-01.csv"]
        mock_read.return_value = pd.DataFrame(
            {"PositiefGetest": [1000], "Opgenomen": [2000], "Overleden": [3000]}
        )

        task = CleanNationalDataset(self.config["collector"], Store())
        task(input_folder="raw", output_folder="interim")

        mock_list.assert_called_once_with("raw/*.csv")
        mock_read.assert_called_once_with("raw/1970-01-01.csv")
        mock_write.assert_called_once_with(
            mock.ANY, "interim/1970-01-01.csv", index=False
        )

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(
                {
                    "PositiefGetest": [1000],
                    "Opgenomen": [2000],
                    "Overleden": [3000],
                    "Datum": ["1970-01-01"],
                }
            ),
            check_dtype=False,
        )


class TestCleanNationalDatasetRead:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd, "read_csv")
    def test_read(self, mock_read_csv, mock_open):
        # pylint: disable=protected-access
        task = CleanNationalDataset(None, Store())
        task._read("test.csv", delimiter=",")

        mock_read_csv.assert_called_once_with(mock.ANY, delimiter=",")
        mock_open.assert_called_once_with("test.csv", "r")


class TestCleanNationalDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = CleanNationalDataset(None, Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

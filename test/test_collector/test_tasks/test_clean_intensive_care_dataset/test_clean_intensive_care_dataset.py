import pandas as pd
import numpy as np
import pytest
import mock

from collector.tasks.clean_intensive_care_dataset.task import CleanIntensiveCareDataset
from collector.schema import ValidationError
from data import create_config
from fixtures import Store


class TestCleanIntensiveCareDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(CleanIntensiveCareDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = CleanIntensiveCareDataset(self.config["collector"], Store())
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
    @mock.patch.object(CleanIntensiveCareDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = CleanIntensiveCareDataset(self.config["collector"], Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Store, "list")
    @mock.patch.object(CleanIntensiveCareDataset, "_read")
    @mock.patch.object(CleanIntensiveCareDataset, "_write")
    def test_run(self, mock_write, mock_read, mock_list):
        mock_list.return_value = [
            "raw/1970-01-01.json",
        ]
        mock_read.side_effect = [
            pd.DataFrame(
                {
                    "date": ["1970-01-01"],
                    "newIntake": [100],
                    "diedCumulative": [200],
                    "survivedCumulative": [300],
                    "intakeCount": [400],
                    "intakeCumulative": [500],
                    "icCount": [600],
                    "icCumulative": [700],
                },
            ),
        ]

        task = CleanIntensiveCareDataset(self.config["collector"], Store())
        task(input_folder="raw", output_folder="interim")

        mock_list.assert_called_once_with("raw/*.json")
        mock_read.assert_called_once_with("raw/1970-01-01.json")
        mock_write.assert_called_once_with(
            mock.ANY, "interim/1970-01-01.csv", index=False
        )

        pd.testing.assert_frame_equal(
            mock_write.call_args_list[0].args[0],
            pd.DataFrame(
                {
                    "Datum": ["1970-01-01"],
                    "NieuwOpgenomen": [100],
                    "OverledenCumulatief": [200],
                    "OverleeftCumulatief": [300],
                    "Opgenomen": [400],
                    "OpgenomenCumulatief": [500],
                    "IntensiveCare": [600],
                    "IntensiveCareCumulatief": [700],
                }
            ),
            check_dtype=False,
        )


class TestCleanIntensiveCareDatasetRead:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd, "read_json")
    def test_read(self, mock_read_json, mock_open):
        # pylint: disable=protected-access
        task = CleanIntensiveCareDataset(None, Store())
        task._read("test.csv")

        mock_read_json.assert_called_once_with(mock.ANY)
        mock_open.assert_called_once_with("test.csv", "r")


class TestCleanIntensiveCareDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = CleanIntensiveCareDataset(None, Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

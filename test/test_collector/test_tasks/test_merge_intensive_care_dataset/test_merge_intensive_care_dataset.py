import pandas as pd
import numpy as np
import pytest
import mock

from fixtures import Store
from data import create_config

from collector.tasks.merge_intensive_care_dataset.task import MergeIntensiveCareDataset
from collector.schema import ValidationError


class TestMergeIntensiveCareDatasetRun:
    @property
    def config(self):
        return create_config()

    @mock.patch.object(MergeIntensiveCareDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = MergeIntensiveCareDataset(self.config["collector"], Store())
        task(name="test", input_folder="interim", output_folder="processed")

        mock_run.assert_called_once_with(
            {"name": "test", "input_folder": "interim", "output_folder": "processed"}
        )

    @pytest.mark.parametrize(
        "inputs,messages",
        [
            (
                {},
                [
                    "'name' is a required property",
                    "'input_folder' is a required property",
                    "'output_folder' is a required property",
                ],
            ),
            (
                {"name": 1, "input_folder": 1, "output_folder": 1},
                [
                    "1 is not of type 'string'",
                    "1 is not of type 'string'",
                    "1 is not of type 'string'",
                ],
            ),
            (
                {"name-1": 1, "input-folder": 1, "output-folder": 1},
                [
                    "'name' is a required property",
                    "'input_folder' is a required property",
                    "'output_folder' is a required property",
                ],
            ),
        ],
    )
    @mock.patch.object(MergeIntensiveCareDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = MergeIntensiveCareDataset(self.config["collector"], Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Store, "list")
    @mock.patch.object(MergeIntensiveCareDataset, "_read")
    @mock.patch.object(MergeIntensiveCareDataset, "_write")
    def test_run(self, mock_write, mock_read, mock_list):
        mock_list.return_value = [
            "interim/1970-01-02-file-1.csv",
            "interim/1970-01-02-file-2.csv",
        ]
        mock_read.side_effect = [
            pd.DataFrame(
                {
                    "Datum": ["1970-01-01", "1970-01-02"],
                    "NieuwOpgenomen": [100, 200],
                    "OverledenCumulatief": [100, 200],
                    "OverleeftCumulatief": [100, 200],
                    "Opgenomen": [100, 200],
                    "OpgenomenCumulatief": [100, 200],
                    "IntensiveCare": [100, 200],
                    "IntensiveCareCumulatief": [100, 200],
                }
            ),
            pd.DataFrame(
                {
                    "Datum": ["1970-01-01", "1970-01-02"],
                    "OverledenCumulatief": [200, 300],
                    "OverleeftCumulatief": [200, 100],
                }
            ),
        ]

        task = MergeIntensiveCareDataset(self.config["collector"], Store())
        task(name="test", input_folder="interim", output_folder="processed")

        mock_list.assert_called_once_with("interim/*.csv")
        mock_read.assert_has_calls(
            [
                mock.call("interim/1970-01-02-file-1.csv"),
                mock.call("interim/1970-01-02-file-2.csv"),
            ]
        )
        mock_write.assert_called_once_with(mock.ANY, "processed/test.csv", index=False)

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(
                {
                    "Datum": ["1970-01-01", "1970-01-02"],
                    "NieuwOpgenomen": [100, 200],
                    "Opgenomen": [100, 200],
                    "OpgenomenCumulatief": [100, 200],
                    "IntensiveCare": [100, 200],
                    "IntensiveCareCumulatief": [100, 200],
                    "OverledenCumulatief": [200, 300],
                    "OverleeftCumulatief": [200, 200],
                }
            ),
            check_dtype=False,
        )


class TestMergeIntensiveCareDatasetRead:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd, "read_csv")
    def test_read(self, mock_read_csv, mock_open):
        # pylint: disable=protected-access
        task = MergeIntensiveCareDataset(None, Store())
        task._read("test.csv", delimiter=",")

        mock_read_csv.assert_called_once_with(mock.ANY, delimiter=",")
        mock_open.assert_called_once_with("test.csv", "r")


class TestMergeIntensiveCareDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = MergeIntensiveCareDataset(None, Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

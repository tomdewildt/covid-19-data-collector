import pandas as pd
import numpy as np
import pytest
import mock

from fixtures import Client, Store
from data import create_config, create_national_response

from collector.tasks.get_national_dataset.task import GetNationalDataset
from collector.schema import ValidationError


class TestGetNationalDatasetRun:
    @property
    def config(self):
        return create_config()

    @property
    def national_response(self):
        return create_national_response()

    @mock.patch.object(GetNationalDataset, "run")
    def test_run_valid_input(self, mock_run):
        task = GetNationalDataset(self.config["collector"], Client(), Store())
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
    @mock.patch.object(GetNationalDataset, "run")
    def test_run_invalid_input(self, mock_run, inputs, messages):
        task = GetNationalDataset(self.config["collector"], Client(), Store())
        with pytest.raises(ValidationError) as error:
            task(**inputs)

        mock_run.assert_not_called()

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]

    @mock.patch.object(Client, "get")
    @mock.patch.object(GetNationalDataset, "_write")
    def test_run(self, mock_write, mock_get):
        mock_get.side_effect = self.national_response

        task = GetNationalDataset(self.config["collector"], Client(), Store())
        task(output_folder="raw")

        mock_get.assert_has_calls(
            [
                mock.call(self.config["collector"]["urls"]["national"]["cases"]),
                mock.call(self.config["collector"]["urls"]["national"]["hospitalized"]),
            ]
        )
        mock_write.assert_called_once_with(mock.ANY, "raw/1970-01-01.csv", index=False)

        pd.testing.assert_frame_equal(
            mock_write.call_args.args[0],
            pd.DataFrame(
                {"PositiefGetest": [1000], "Opgenomen": [2000], "Overleden": [3000]}
            ),
            check_dtype=False,
        )


class TestGetNationalDatasetWrite:
    @mock.patch.object(Store, "open")
    @mock.patch.object(pd.DataFrame, "to_csv")
    def test_write(self, mock_to_csv, mock_open):
        # pylint: disable=protected-access
        task = GetNationalDataset(None, Client(), Store())
        task._write(pd.DataFrame(np.zeros(shape=(3, 3))), "test.csv", index=False)

        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        mock_open.assert_called_once_with("test.csv", "w")

import logging
import os

import pandas as pd
import numpy as np

from collector.schema import obj, string, validate
from collector.utils import filter_files

log = logging.getLogger(__name__)


class MergeIntensiveCareDataset:
    inputs_schema = obj(name=string(), input_folder=string(), output_folder=string())

    def __init__(self, config, store):
        self._config = config
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        log.info("Retrieving datasets")
        files = filter_files(self._store.list(f"{inputs['input_folder']}/*.csv"))

        log.info("Loading dataset")
        data = self._read(f"{inputs['input_folder']}/{os.path.basename(files[0])}")

        log.info("Merging datasets")
        for file in files[1:]:
            file = os.path.basename(file)

            # Load dataset
            data = data.merge(
                self._read(f"{inputs['input_folder']}/{file}"), how="outer", on="Datum"
            )

            # Clean columns
            columns = list(filter(lambda c: "_" in c in c, data.columns.to_list()))
            for column in columns:
                column = column[:-2]

                data[column] = np.max(data[[column + "_x", column + "_y"]], axis=1)

            # Drop columns
            data = data.drop(columns, axis=1)

            # Fix values
            data[data.columns.difference(["Datum"])] = (
                data[data.columns.difference(["Datum"])].fillna(0).astype(int)
            )

        log.info("Sorting dataset")
        data = data.sort_values(["Datum"])

        log.info("Storing dataset")
        path = f"{inputs['output_folder']}/{inputs['name']}.csv"

        self._write(data, path, index=False)

    def _read(self, path, **kwargs):
        with self._store.open(path, "r") as handle:
            return pd.read_csv(handle, **kwargs)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"

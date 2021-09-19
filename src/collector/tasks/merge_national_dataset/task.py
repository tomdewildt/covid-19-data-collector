import logging
import os

import pandas as pd

from collector.schema import obj, string, validate

log = logging.getLogger(__name__)


class MergeNationalDataset:
    inputs_schema = obj(name=string(), input_folder=string(), output_folder=string())

    def __init__(self, config, store):
        self._config = config
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        data = pd.DataFrame(
            columns=["PositiefGetest", "Opgenomen", "Overleden", "Datum"]
        )

        log.info("Merging datasets")
        for file in self._store.list(f"{inputs['input_folder']}/*.csv"):
            file = os.path.basename(file)

            data = data.append(
                self._read(f"{inputs['input_folder']}/{file}"), ignore_index=True
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

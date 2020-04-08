import logging
import os

import pandas as pd

from collector.schema import obj, string, validate

log = logging.getLogger(__name__)


class CleanMunicipalityDataset:
    inputs_schema = obj(input_folder=string(), output_folder=string())

    def __init__(self, config, store):
        self._config = config
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        log.info("Loading municipalities")
        municipalities = self._read(self._config["municipalities"])

        log.info("Loading datasets")
        for file in self._store.list(f"{inputs['input_folder']}/*.csv"):
            file = os.path.basename(file)

            # Load dataset
            data = self._read(f"{inputs['input_folder']}/{file}")

            # Rename columns
            data = data.rename(
                columns={
                    "id": "Gemeentecode",
                    "Gemnr": "Gemeentecode",
                    "Zkh opname": "Aantal",
                }
            )
            data["Gemeentecode"] = data["Gemeentecode"].astype(int)

            # Fix missing cases
            cell = data.loc[data["Gemeentecode"] == -1, "Gemeente"]
            if len(cell.values) > 0:
                amount = sum(int(s) for s in cell.values[0].split() if s.isdigit())
                data.loc[data["Gemeentecode"] == -1, "Aantal"] = amount

            # Select columns
            data = data[["Gemeentecode", "Aantal"]]

            # Merge municipality data
            data = data.merge(
                municipalities,
                left_on="Gemeentecode",
                right_on="Gemeentecode",
                how="left",
            )

            # Add datum column
            data["Datum"] = os.path.splitext(file)[0]

            # Fill empty values
            data = data.fillna({"Provinciecode": -1, "Gemeentecode": -1, "Aantal": 0,})
            data.loc[data["Provinciecode"] == -1, "Provincie"] = None
            data.loc[data["Gemeentecode"] == -1, "Gemeente"] = None

            # Set data types
            data["Provinciecode"] = data["Provinciecode"].astype(int)
            data["Gemeentecode"] = data["Gemeentecode"].astype(int)
            data["Aantal"] = data["Aantal"].astype(int)

            # Store dataset
            path = f"{inputs['output_folder']}/{file}"

            self._write(data, path, index=False)

    def _read(self, path, **kwargs):
        with self._store.open(path, "r") as handle:
            return pd.read_csv(handle, **kwargs)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return "<{}()>".format(self.__class__.__name__)

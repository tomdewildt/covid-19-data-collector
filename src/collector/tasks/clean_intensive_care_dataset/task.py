import logging
import os

import pandas as pd

from collector.schema import obj, string, validate

log = logging.getLogger(__name__)


class CleanIntensiveCareDataset:
    inputs_schema = obj(input_folder=string(), output_folder=string())

    def __init__(self, config, store):
        self._config = config
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        log.info("Loading datasets")
        for file in self._store.list(f"{inputs['input_folder']}/*.json"):
            file = os.path.basename(file)

            # Load dataset
            data = self._read(f"{inputs['input_folder']}/{file}")

            # Rename columns
            data = data.rename(
                columns={
                    "newIntake": "NieuwOpgenomen",
                    "intakeCount": "Opgenomen",
                    "intakeCumulative": "OpgenomenCumulatief",
                    "icCount": "IntensiveCare",
                    "icCumulative": "IntensiveCareCumulatief",
                    "diedCumulative": "OverledenCumulatief",
                    "survivedCumulative": "OverleeftCumulatief",
                    "date": "Datum",
                }
            )

            # Store dataset
            path = f"{inputs['output_folder']}/{file.split('.')[0]}.csv"

            self._write(data, path, index=False)

    def _read(self, path, **kwargs):
        with self._store.open(path, "r") as handle:
            return pd.read_json(handle, **kwargs)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return "<{}()>".format(self.__class__.__name__)

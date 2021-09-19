import logging
import io

import pandas as pd

from collector.schema import obj, string, validate

log = logging.getLogger(__name__)


class GetNationalDataset:
    inputs_schema = obj(output_folder=string())

    def __init__(self, config, client, store):
        self._config = config
        self._client = client
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        log.info("Requesting document")
        document = self._client.get(self._config["urls"]["national"])
        buffer = io.StringIO(document)

        log.info("Parsing document")
        data = pd.read_csv(buffer, delimiter=";")
        date = data["Date_of_publication"].iat[-1]

        log.info("Parsing data")
        data = data[data["Date_of_publication"] == date]
        data = data.dropna(subset=["Municipality_code"])
        data = pd.DataFrame(
            {
                "PositiefGetest": [data["Total_reported"].sum()],
                "Opgenomen": [data["Hospital_admission"].sum()],
                "Overleden": [data["Deceased"].sum()],
            }
        )
        date = date.split()[0]

        log.info("Storing dataset")
        path = f"{inputs['output_folder']}/{date}.csv"

        self._write(data, path, index=False)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"

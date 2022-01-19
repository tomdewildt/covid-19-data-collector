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
        log.info("Requesting cases document")
        cases_document = self._client.get(self._config["urls"]["national"]["cases"])
        cases_buffer = io.StringIO(cases_document)

        log.info("Requesting hospitalized document")
        hospitalized_document = self._client.get(
            self._config["urls"]["national"]["hospitalized"],
        )
        hospitalized_buffer = io.StringIO(hospitalized_document)

        log.info("Parsing cases document")
        cases_data = pd.read_csv(cases_buffer, delimiter=";")
        cases_date = cases_data["Date_of_publication"].iat[-1]

        log.info("Parsing hospitalized document")
        hospitalized_data = pd.read_csv(hospitalized_buffer, delimiter=";")
        hospitalized_date = hospitalized_data["Date_of_statistics"].iat[-1]

        log.info("Parsing cases data")
        cases_data = cases_data[cases_data["Date_of_publication"] == cases_date]
        cases_data = cases_data.dropna(subset=["Municipality_code"])

        log.info("Parsing hospitalized data")
        hospitalized_data = hospitalized_data[
            hospitalized_data["Date_of_statistics"] == hospitalized_date
        ]
        hospitalized_data = hospitalized_data.dropna(subset=["Municipality_code"])

        log.info("Merging data")
        data = pd.DataFrame(
            {
                "PositiefGetest": [cases_data["Total_reported"].sum()],
                "Opgenomen": [hospitalized_data["Hospital_admission"].sum()],
                "Overleden": [cases_data["Deceased"].sum()],
            }
        )

        log.info("Storing dataset")
        path = f"{inputs['output_folder']}/{cases_date}.csv"

        self._write(data, path, index=False)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__}()>"

import logging
import json

from bs4 import BeautifulSoup
import pandas as pd

from collector.schema import obj, string, validate
from collector.utils import format_date

log = logging.getLogger(__name__)


class GetNationalDatasetError(Exception):
    pass


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

        log.info("Parsing document")
        soup = BeautifulSoup(document, features="html.parser")
        data_element = soup.find(self._config["elements"]["national"])
        metadata_element = soup.find(id=self._config["elements"]["metadata"])
        if data_element is None:
            raise GetNationalDatasetError("Data element not found in document")
        if metadata_element is None:
            raise GetNationalDatasetError("Metadata element not found in document")

        log.info("Parsing data")
        data = pd.DataFrame(columns=["PositiefGetest", "Opgenomen", "Overleden"])
        for (idx, row) in enumerate(data_element.findAll("tr")):
            element = row.findAll("td")[1].find("h4")
            amount = element.text.split()[0].replace(".", "").replace("*", "")

            data.at[0, data.columns[idx]] = int(amount)

        metadata = metadata_element.text
        metadata = json.loads(metadata)

        log.info("Storing dataset")
        path = f"{inputs['output_folder']}/{format_date(metadata)}.csv"

        self._write(data, path, index=False)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            data.to_csv(handle, **kwargs)

    def __repr__(self):
        return "<{}()>".format(self.__class__.__name__)

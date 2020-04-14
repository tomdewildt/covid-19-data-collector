import logging
import json

from collector.schema import obj, string, validate

log = logging.getLogger(__name__)


class GetIntensiveCareDataset:
    inputs_schema = obj(output_folder=string())

    def __init__(self, config, client, store):
        self._config = config
        self._client = client
        self._store = store

    def __call__(self, **kwargs):
        validate(self.inputs_schema, kwargs)
        return self.run(kwargs)

    def run(self, inputs):
        for url in self._config["urls"]["intensive_care"]:
            name = url.split("/")[-1]

            log.info("Downloading %s document", name)
            document = self._client.get(url)

            log.info("Parsing document")
            data = json.loads(document)

            if name == "new-intake":
                log.info("Storing dataset")
                date = data[0][-1]["date"]
                path = f"{inputs['output_folder']}/{date}-new-intake-confirmed.json"

                self._write(data[0], path)

                log.info("Storing dataset")
                date = data[0][-1]["date"]
                path = f"{inputs['output_folder']}/{date}-new-intake-suspicious.json"

                self._write(data[0], path)
            elif name == "died-and-survivors-cumulative":
                log.info("Storing dataset")
                date = data[0][-1]["date"]
                path = f"{inputs['output_folder']}/{date}-died-cumulative.json"

                self._write(data[0], path)

                log.info("Storing dataset")
                date = data[0][-1]["date"]
                path = f"{inputs['output_folder']}/{date}-survived-cumulative.json"

                self._write(data[0], path)
            else:
                log.info("Storing dataset")
                date = data[-1]["date"]
                path = f"{inputs['output_folder']}/{date}-{name}.json"

                self._write(data, path)

    def _write(self, data, path, **kwargs):
        with self._store.open(path, "w") as handle:
            json.dump(data, handle, **kwargs)

    def __repr__(self):
        return "<{}()>".format(self.__class__.__name__)

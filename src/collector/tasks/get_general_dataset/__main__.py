import click

from collector.config import read_config, init_logging
from collector.tasks.get_general_dataset.task import GetGeneralDataset
from collector.client import WebClient
from collector.store import LocalStore


@click.command()
@click.option("--output_folder", help="The folder where the dataset should be stored")
def main(output_folder):
    config = read_config()
    client = WebClient()
    store = LocalStore(config["store"]["path"])

    task = GetGeneralDataset(config["collector"], client, store)
    task(output_folder=output_folder)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    init_logging()
    main()

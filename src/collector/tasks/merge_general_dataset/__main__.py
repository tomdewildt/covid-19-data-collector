import click

from collector.config import read_config, init_logging
from collector.tasks.merge_general_dataset.task import MergeGeneralDataset
from collector.store import LocalStore


@click.command()
@click.option("--name", help="The name of the dataset")
@click.option("--input_folder", help="The folder containing the dataset files")
@click.option("--output_folder", help="The folder where the dataset should be stored")
def main(name, input_folder, output_folder):
    config = read_config()
    store = LocalStore(config["store"]["path"])

    task = MergeGeneralDataset(config["collector"], store)
    task(name=name, input_folder=input_folder, output_folder=output_folder)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    init_logging()
    main()

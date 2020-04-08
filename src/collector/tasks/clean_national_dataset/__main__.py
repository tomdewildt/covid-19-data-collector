import click

from collector.config import read_config, init_logging
from collector.tasks.clean_national_dataset.task import CleanNationalDataset
from collector.store import LocalStore


@click.command()
@click.option("--input_folder", help="The folder containing the dataset files")
@click.option("--output_folder", help="The folder where the dataset should be stored")
def main(input_folder, output_folder):
    config = read_config()
    store = LocalStore(config["store"]["path"])

    task = CleanNationalDataset(config["collector"], store)
    task(input_folder=input_folder, output_folder=output_folder)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    init_logging()
    main()

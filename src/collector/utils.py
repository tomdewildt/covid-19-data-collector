from datetime import datetime
import os


def format_date(metadata):
    text = metadata["nl"]["mapSubtitle"]

    date = datetime.strptime(text.split()[-1], "%d-%m-%Y")

    return date.strftime("%Y-%m-%d")


def filter_files(files):
    max_date = datetime(1970, 1, 1)

    for file in files:
        file = os.path.basename(file)
        date = datetime.strptime(file[0:10], "%Y-%m-%d")

        if date >= max_date:
            max_date = date

    return list(filter(lambda f: max_date.strftime("%Y-%m-%d") in f, files))

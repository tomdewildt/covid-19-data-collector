from datetime import datetime


def format_date(metadata):
    text = metadata["nl"]["mapSubtitle"]

    date = datetime.strptime(text.split()[-1], "%d-%m-%Y")

    return date.strftime("%Y-%m-%d")

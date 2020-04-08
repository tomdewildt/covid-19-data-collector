# COVID-19 Data Collector
![CI Status](https://github.com/tomdewildt/covid-19-data-collector/workflows/ci/badge.svg?branch=master)
![Scheduler Status](https://github.com/tomdewildt/covid-19-data-collector/workflows/scheduler/badge.svg?branch=master)
[![Coverage Status](https://codecov.io/gh/tomdewildt/covid-19-data-collector/branch/master/graph/badge.svg)](https://codecov.io/gh/tomdewildt/covid-19-data-collector)

This tool automatically collects and parses the data from the RIVM website on the COVID-19 oubreak in The Netherlands.

# How To Run

Prerequisites:
* virtualenv version ```20.0.3``` or later
* python version ```3.6.9``` or later
* pylint version ```2.4.4``` or later
* black version ```19.10b0``` or later

### Development

1. Run ```make init``` to initialize the environment.
2. Run ```make run/[task]``` to execute a single task.

#### Available tasks

* ```get_national_dataset``` retrieves national outbreak data.
* ```get_municipality_dataset``` retrieves outbreak data per municipality.
* ```clean_national_dataset``` clean the national datasets.
* ```clean_municipality_dataset``` clean the municipality datasets.
* ```merge_national_dataset``` merge the national datasets.
* ```merge_municipality_dataset``` merge the municipality datasets.

### Test

1. Run ```make init``` to initialize the environment.
2. Run ```make test``` to execute the tests.

# References

[RIVM COVID-19](https://www.rivm.nl/coronavirus-covid-19/actueel)

[Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

[Pandas](https://pandas.pydata.org/)

[Numpy](https://numpy.org/)

[Pytest](https://docs.pytest.org/en/latest/)

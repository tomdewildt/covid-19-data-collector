# COVID-19 Data Collector
[![Build](https://img.shields.io/github/workflow/status/tomdewildt/covid-19-data-collector/ci/master)](https://github.com/tomdewildt/covid-19-data-collector/actions?query=workflow%3Aci)
[![Scheduler](https://img.shields.io/github/workflow/status/tomdewildt/covid-19-data-collector/scheduler/master?label=scheduler)](https://github.com/tomdewildt/covid-19-data-collector/actions?query=workflow%3Ascheduler)
[![Coverage](https://img.shields.io/codecov/c/gh/tomdewildt/covid-19-data-collector)](https://codecov.io/gh/tomdewildt/covid-19-data-collector)
![Size](https://img.shields.io/github/repo-size/tomdewildt/covid-19-data-collector)
[![License](https://img.shields.io/github/license/tomdewildt/covid-19-data-collector)](https://github.com/tomdewildt/covid-19-data-collector/blob/master/LICENSE)

> **DEPRECATED**
> 
> This tool is deprecated in favor of the government provided [dashboard](https://coronadashboard.rijksoverheid.nl/) and [data](https://data.rivm.nl/covid-19/).

This tool automatically collects and parses the data from the RIVM and NICE websites on the COVID-19 oubreak in The Netherlands.

# How To Run

Prerequisites:
* virtualenv version ```20.0.3``` or later
* python version ```3.8.5``` or later
* pylint version ```2.4.4``` or later
* black version ```19.10b0``` or later

### Development

1. Run ```make init``` to initialize the environment.
2. Run ```make run/[task]``` to execute a single task.

#### Available tasks

* ```get_national_dataset``` retrieves national outbreak data.
* ```get_municipality_dataset``` retrieves outbreak data per municipality.
* ```get_intensive_care_dataset``` retrieves intensive care data.
* ```clean_national_dataset``` clean the national datasets.
* ```clean_municipality_dataset``` clean the municipality datasets.
* ```clean_intensive_care_dataset``` clean the intensive care datasets.
* ```merge_national_dataset``` merge the national datasets.
* ```merge_municipality_dataset``` merge the municipality datasets.
* ```merge_intensive_care_dataset``` merge the intensive care datasets.

### Test

1. Run ```make init``` to initialize the environment.
2. Run ```make test``` to execute the tests.

# Datasets

This repository contains three datasets that are updated every day. The data is collected from the RIVM and NICE websites.

The data folder contains four subfolders:

* `raw` contains the raw datasets.
* `interim` contains contains the cleaned datasets.
* `processed` contains the merged datasets.
* `external` contains external datasets.

| Dataset                                                                                                                                               | Source | Fields                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [rivm-covid-19-national.csv](https://github.com/tomdewildt/covid-19-data-collector/blob/master/data/processed/rivm-covid-19-national.csv)             | RIVM   | Confirmed Cases (`PositiefGetest`), Hospitalized (`Opgenomen`), Deceased (`Overleden`), Date (`Datum`)                                                                                                                                                                                                                                  |
| [rivm-covid-19-municipality.csv](https://github.com/tomdewildt/covid-19-data-collector/blob/master/data/processed/rivm-covid-19-municipality.csv)     | RIVM   | Municipality Code (`Gemeentecode`), Confirmed Cases (`PositiefGetest`), Municipality (`Gemeente`), Province Code (`Provinciecode`), Province (`Provincie`), Date (`Datum`)                                                                                                                                                              |
| [nice-covid-19-intensive-care.csv](https://github.com/tomdewildt/covid-19-data-collector/blob/master/data/processed/nice-covid-19-intensive-care.csv) | NICE   | Date (`Datum`), Hospitalized Cumulative (`OpgenomenCumulatief`), Intensive Care (`Intensive Care`), Survived Cumulative (`OverleeftCumulatief`), Deceased Cumulative (`OverledenCumulatief`), Hospitalized (`Opgenomen`), Newly Hospitalized Suspicious (`NieuwOpgenomenVerdacht`), Newly Hospitalized Proven (`NieuwOpgenomenBewezen`) |

# References

[RIVM COVID-19](https://www.rivm.nl/coronavirus-covid-19/actueel)

[NICE COVID-19](https://www.stichting-nice.nl/)

[Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

[Pandas](https://pandas.pydata.org/)

[Numpy](https://numpy.org/)

[Pytest](https://docs.pytest.org/en/latest/)

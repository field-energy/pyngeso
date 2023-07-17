# pyngeso

Simple python wrapper for the National Grid ESO Portal.

[![](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/pyenv/pyenv)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

## Getting started


* Example usage
```python
from pyngeso import NgEso

resource = "historic-day-ahead-demand-forecast"
date_col = "TARGETDATE"
start_date = "2018-01-01"
end_date = "2018-01-01"

client = NgEso(resource)
# returns content of response
r: bytes = client.query(date_col=date_col, start_date=start_date, end_date=end_date)
```

## Tested reports

### Queryable via NG's api
* `historic-day-ahead-demand-forecast`
* `day-ahead-demand-forecast`
* `historic-2day-ahead-demand-forecast`
* `2day-ahead-demand-forecast`
* `historic-2-14-days-ahead-demand-forecast`
* `historic-day-ahead-wind-forecast`
* `day-ahead-wind-forecast`
* `14-days-ahead-wind-forecast`
* `demand-data-update`
* `dc-results-summary`
* `dc-dr-dm-linear-orders`
* `historic-demand-data-{year}` [2009-2022]
* `historic-frequency-data` [Jan21-Jan22]
* `transmission-entry-capacity-tec-register`

### Download of files
* `historic-generation-mix`

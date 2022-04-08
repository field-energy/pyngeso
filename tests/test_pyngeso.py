from datetime import datetime
import logging
import json
import csv

import pytest

from pyngeso import NgEso
from pyngeso.exceptions import UnsuccessfulRequest


@pytest.mark.vcr
def test_day_ahead_historic_forecast():
    start_date = "2018-01-02"
    end_date = "2018-01-02"
    client = NgEso("historic-day-ahead-demand-forecast")
    r = client.query(date_col="TARGETDATE", start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get("TARGETDATE") for record in records])
    assert len(unique_target_dates) == 1


@pytest.mark.vcr
def test_query_unsuccessful_request(caplog):
    with pytest.raises(UnsuccessfulRequest) as exc_info:
        client = NgEso("historic-day-ahead-demand-forecast")
        _ = client.query(date_col="no_such_col", end_date="2018-01-02")
    assert "status_code=4" in str(exc_info.value)


@pytest.mark.vcr
def test_day_ahead_historic_forecast_missing_data_warning(caplog):
    with caplog.at_level(logging.WARNING):
        client = NgEso("historic-day-ahead-demand-forecast")
        _ = client.query(date_col="TARGETDATE", start_date="2050-01-02")
    assert "No data found" in caplog.text


@pytest.mark.vcr
def test_2day_ahead_historic_forecast():
    start_date = "2018-03-30"
    end_date = "2018-03-30"
    client = NgEso("historic-2day-ahead-demand-forecast")
    r = client.query(date_col="TARGETDATE", start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get("TARGETDATE") for record in records])
    assert len(unique_target_dates) == 1


@pytest.mark.vcr
def test_historic_day_ahead_wind_forecast():
    date_col = "Date"
    start_date = "2018-04-17"
    end_date = "2018-04-17"
    client = NgEso("historic-day-ahead-wind-forecast")
    r = client.query(date_col=date_col, start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1


@pytest.mark.vcr
def test_historic_generation_mix():
    client = NgEso("historic-generation-mix", "file")
    r = client.download_file()
    # test response type
    assert isinstance(r, bytes)

    # test bytes -> csv
    decoded_content = r.decode('utf-8')
    c = csv.reader(decoded_content.splitlines(), delimiter=',')
    headers_row = next(c)
    first_row = next(c)

    assert "DATETIME" in headers_row
    assert "2009-01-01 00:00:00" in first_row
    assert len(headers_row) == len(first_row)


@pytest.mark.vcr
def test_demand_data_update():
    date_col = "SETTLEMENT_DATE"
    start_date = "2021-10-01"
    end_date = "2021-10-01"
    client = NgEso("demand-data-update")
    r = client.query(date_col=date_col, start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1
    assert len(records) == 48


@pytest.mark.vcr
def test_demand_data_update_with_filter():
    date_col = "SETTLEMENT_DATE"
    start_date = "2021-10-01"
    end_date = "2021-10-01"
    filter_condition = "\"FORECAST_ACTUAL_INDICATOR\" = 'A'"
    client = NgEso("demand-data-update")
    r = client.query(date_col=date_col, start_date=start_date, end_date=end_date,
                     filters=[filter_condition])

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1
    assert len(records) == 48


@pytest.mark.vcr
def test_dc_results_summary():
    date_col = "EFA Date"
    start_date = "2021-09-16"
    end_date = "2021-09-16"
    filter_condition = "\"Service\" = 'DCH'"
    client = NgEso("dc-results-summary")
    r = client.query(date_col=date_col, start_date=start_date, end_date=end_date,
                     filters=[filter_condition])

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1
    assert len(records) == 6


@pytest.mark.vcr
@pytest.mark.parametrize(
    "month, year",
    [
        ("jan", 2022),
        ("jan", 2021),
        ("feb", 2021),
        ("mar", 2021),
        ("apr", 2021),
        ("may", 2021),
        ("jun", 2021),
        ("jul", 2021),
        ("aug", 2021),
        ("sep", 2021),
        ("oct", 2021),
        ("nov", 2021),
        ("dec", 2021),
    ],
)
def test_historic_frequency_data(month: str, year: int):
    date_col = "dtm"
    start_date = datetime.strptime(f"{month} {year}", "%b %Y").strftime("%Y-%m-%d")
    client = NgEso(f"historic-frequency-data-{month}{str(year)[-2:]}")
    r = client.query(date_col=date_col, start_date=start_date, end_date=start_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1
    assert len(records) == 1
    fetched_year = int(records[0].get(date_col)[:4])
    assert fetched_year == year
    
    
@pytest.mark.vcr
@pytest.mark.parametrize(
    "year",
    [
        2009,
        2010,
        2011,
        2012,
        2013,
        2014,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
        2021,
        2022
    ],
)
def test_historic_demand_data(year: int):
    date_col = "SETTLEMENT_DATE"
    start_date = datetime.strptime(f"{year}", "%Y").strftime("%Y-%m-%d")
    client = NgEso(f"historic-demand-data-{year}")
    r = client.query(date_col=date_col, start_date=start_date, end_date=start_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get(date_col) for record in records])
    assert len(unique_target_dates) == 1
    assert len(records) == 1
    fetched_year = int(records[0].get(date_col)[:4])
    assert fetched_year == year

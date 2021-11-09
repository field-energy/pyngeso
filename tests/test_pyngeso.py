import logging
import json

import pytest

from pyngeso import NgEso
from pyngeso.exceptions import UnsuccessfulRequest


@pytest.mark.vcr
def test_day_ahead_historic_forecast():
    start_date = "2018-01-01"
    end_date = "2018-01-02"
    client = NgEso("historic-day-ahead-demand-forecast")
    r = client.query(date_col="TARGETDATE", start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get("TARGETDATE") for record in records])
    assert len(unique_target_dates) == 2


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
    end_date = "2018-03-31"
    client = NgEso("historic-2day-ahead-demand-forecast")
    r = client.query(date_col="TARGETDATE", start_date=start_date, end_date=end_date)

    assert isinstance(r, bytes)
    r_dict = json.loads(r)
    records = r_dict.get("result").get("records")
    assert isinstance(records, list)
    assert len(records) > 0
    unique_target_dates = set([record.get("TARGETDATE") for record in records])
    assert len(unique_target_dates) == 2

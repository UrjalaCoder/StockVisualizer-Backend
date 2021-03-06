import pandas as pd
import os
import requests
from datetime import datetime
import math
from consumers.utils import parse_to_datetime, parse_epoch_to_datetime, partition_with_dates

# Determines the url
def formulate_url(symbol, full=False, time_interval=1, daily=False):
    key = os.getenv("AV_KEY", None)
    data_type = "TIME_SERIES_DAILY" if daily else "TIME_SERIES_INTRADAY"
    api_endpoint = f"https://www.alphavantage.co/query?function={data_type}"
    size = "full" if full else "compact"
    interval = str(time_interval)
    option_url = f"&symbol={symbol}&outputsize={size}&datatype=json"
    interval_url = "" if daily else f"interval={interval}min"
    key_option = f"apikey={key}"
    full_url = f"{api_endpoint}{option_url}&{interval_url}&{key_option}"
    return (full_url, key)

# Get the raw data
def get_raw_data(symbol="MSFT", full=False, time_interval=1, daily=False):
    url, key = formulate_url(symbol, full=full, time_interval=time_interval, daily=daily)
    if key is None:
        return
    request = requests.get(url)
    if request.status_code != 200:
        return
    return request.json()

def filter_data(row, datetime_object):
    date = row["date"]
    data_datetime = parse_to_datetime(date)
    return datetime_object < data_datetime

# Transform the data
def formulate_data(raw_data, datetime):
    meta_key, data_key = list(raw_data.keys())
    real_data = raw_data[data_key]
    dataframe = pd.DataFrame(real_data)
    dataframe = dataframe.transpose()

    # Set correct columns
    cols = ["date", "open", "high", "low", "close", "volume"]
    dataframe = dataframe.reset_index()
    dataframe.columns = cols
    # Filter the data using the datetime
    dataframe_filter = dataframe.apply(lambda row: filter_data(row, datetime), axis=1)
    dataframe = dataframe[dataframe_filter]
    
    # The data is now transformed
    return dataframe

# Main consumer
def consume(symbol="MSFT", last_datestamp=None):
    time_interval = 60
    compact_size = 100
    date = parse_epoch_to_datetime(last_datestamp) if last_datestamp is not None else parse_epoch_to_datetime(1)
    
    # Deduce if we have to get the full history
    minute_difference = int(math.ceil((datetime.utcnow().timestamp() - date.timestamp()) / 60))
    # If the minute difference is larger than the time interval * compact size
    # get the whole history
    full = minute_difference >= (time_interval * compact_size)
    
    # Get the data
    raw = get_raw_data(symbol, full=full, time_interval=time_interval)
    result_data = formulate_data(raw, date)
    return result_data

if __name__ == "__main__":
    data = consume(symbol="MSFT")
    partitions = partition_with_dates(data)

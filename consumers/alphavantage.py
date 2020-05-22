import pandas as pd
from collections import namedtuple
import os

def formulate_url(symbol, full=False):
    key = os.getenv("AV_KEY", None)
    # Determine the url
    api_endpoint = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY"
    size = "full" if full else "compact"
    interval = str(5)
    option_url = f"&symbol={symbol}&interval={interval}min&outputsize={size}&datatype=json"
    key_option = f"apikey={key}"
    full_url = f"{api_endpoint}{option_url}&{key_option}"
    return (full_url, key)

def get_raw_data(symbol="MSFT", stamp=0):
    url, key = formulate_url(symbol)
    # Get the data

# Return list of datapoints (named tuples)
def consume(symbol="MSFT", stamp=None):
    datapoints = []
    d1 = namedtuple("Datapoint", ['low', 'high'])
    d1.low = 0
    d1.high = 1
    datapoints.append(d1)
    return datapoints

if __name__ == "__main__":
    url, key = formulate_url("MSFT", False)
    print(url)

import pandas as pd
from collections import namedtuple
import os

def formulate_url(symbol, full=False):
    key = os.getenv("AV_KEY", None)
    if key is None:
        return None

def get_raw_data(symbol="MSFT", stamp=0):
    url = formulate_url(symbol)
    if url is None:
        return None
    

# Return list of datapoints (named tuples)
def consume(symbol="MSFT", stamp=None):
    datapoints = []
    d1 = namedtuple("Datapoint", ['low', 'high'])
    d1.low = 0
    d1.high = 1
    datapoints.append(d1)
    return datapoints

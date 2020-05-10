import pandas as pd
import requests
from datetime import datetime

# Formulate the url to be used
def formulate_url(instrument, secret):
    return f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={instrument}&outputsize=full&apikey={secret}"

# Filter the results based on dates
def filter_results(df, time_start, time_end):
    def compute_stamp(datestring):
        year, month, d = list(map(lambda s: int(s), datestring.split("-")))
        date_object = datetime(year, month, d)
        return date_object.timestamp()
    df["Date"] = df["Date"].map(compute_stamp)
    df = df[(df.Date <= time_end) & (df.Date >= time_start)]
    df.reset_index(level=0, inplace=True, drop=True)
    return df

def rename_columns(df):
    real_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
    replacing_dict = dict(list(zip(df.columns, real_columns)))
    df = df.rename(columns=replacing_dict)
    return df

def get_API_data(instrument, time_start, time_end, secret):
    # TODO: Error handling, (404 and the like).
    url = formulate_url(instrument, secret)
    response_data = requests.get(url)
    json_data = response_data.json()
    real_data = json_data["Time Series (Daily)"]
    dataframe = pd.DataFrame(real_data).transpose()
    dataframe.index.name = "Date"
    dataframe.reset_index(level=0, inplace=True)
    final_result = filter_results(dataframe, 0, 1000000000)
    final_result = rename_columns(final_result)
    return final_result

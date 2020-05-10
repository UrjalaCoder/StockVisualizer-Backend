import numpy as np
import pandas as pd
from key import API_SECRET
from api_handler import get_API_data

# Get data from the internal storage
def get_from_memory(instrument, time_start, time_end):
    pass

# Get data from the outside API
def get_from_external(instrument, time_start, time_end):
    return get_API_data(instrument, time_start, time_end, API_SECRET)

# Store the result to memory
def store_to_memory(instrument):
    pass

# Main function for getting the data for an instrument
def get_data(instrument, time_start, time_end):
    current_data = pd.read_csv("memory_times.csv")
    # print(current_data)
    start_stamp, end_stamp = current_data["start"][0], current_data["end"][0]
    # If the date range is not in the current timeframe, get from external
    # print(start_stamp)
    data = None
    if start_stamp <= time_start and time_end <= end_stamp:
        print("In range, getting from internal memory")
        data = get_from_memory(instrument, time_start, time_end)
        if data is None:
            data = get_from_external(instrument, time_start, time_end)
    else:
        print("Not in range, accessing external API...")
        data = get_from_external(instrument, time_start, time_end)
    return data

if __name__ == "__main__":
    get_data("MSFT", 100, 200)

import firebase_admin
from firebase_admin import firestore
from datetime import datetime
import consumers.utils as utils

# Initialization
firebase_admin.initialize_app()
db = firestore.client()

def format_dataframe(datapoints):
    return datapoints.set_index("date").to_dict("index")

def save_data(datapoints, symbol="MSFT"):
    # Partition based on dates:
    partitions = utils.partition_with_dates(datapoints)
    for timestamp in partitions.keys():
        date_object = utils.parse_epoch_to_datetime(timestamp)
        date_string = utils.format_to_date(date_object)
        partition_data = partitions[timestamp]
        partition_df = format_dataframe(partition_data)
        db.collection(f"{symbol}-data").document(f"{date_string}").set(partition_df)

def save_lasttime(datapoints, symbol="MSFT"):
    if len(datapoints.index) < 1:
        return
    # Get the timestamps
    date_rows = datapoints.apply(lambda row: utils.parse_to_datetime(row["date"]), axis=1)
    stamps = date_rows.apply(lambda val: val.timestamp())
    # Maximum timestamp
    max_timestamp = stamps.max()
    db.collection(f"{symbol}-data").document(f"last_time").set({
       "time": max_timestamp
    })

def last_time(symbol="MSFT"):
    data_dict = db.collection(f"{symbol}-data").document(f"last_time").get()
    if data_dict.exists:
        result = int(data_dict.to_dict()["time"])
        return result
    return 1

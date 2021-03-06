import datetime

# Partitioning on dates
def partition_with_dates(raw_data):
    partitions = {}
    dates = raw_data["date"]
    data_partitions = {}
    # Get the different dates
    for index, row in raw_data.iterrows():
        date = parse_to_datetime(row["date"])
        start_of_date = datetime.datetime(date.year, date.month, date.day)
        start_timestamp = start_of_date.replace(tzinfo=datetime.timezone.utc).timestamp()
        # Get all the different start dates
        if start_timestamp not in partitions:
            partitions[start_timestamp] = True
    
    # Now partition the data
    def filter(row, target_date):
        data_date = parse_to_datetime(row["date"])
        a = target_date.day == data_date.day
        b = target_date.month == data_date.month
        c = target_date.year == target_date.year
        return a and b and c
    
    for partition in partitions.keys():
        target_date = parse_epoch_to_datetime(partition)
        partitioned = raw_data.apply(lambda row: filter(row, target_date), axis=1)
        partitioned_data = raw_data[partitioned]
        data_partitions[partition] = partitioned_data
    
    return data_partitions

def format_to_date(d):
    return d.strftime("%Y-%m-%d")

def parse_to_datetime(string):
    return datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")

def parse_epoch_to_datetime(unix_stamp):
    return datetime.datetime.utcfromtimestamp(unix_stamp)

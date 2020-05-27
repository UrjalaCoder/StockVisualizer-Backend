import pandas as pd
from time import sleep
from consumers import alphavantage as av
from database_utils import database_handler as dbh

# Get the symbols
def load_symbols():
    symbol_df = pd.read_csv("./consumers/symbols.csv", index_col=False)
    return symbol_df

# Get the data for one symbol and add them to the database
def consume_one(symbol="MSFT"):
    print(f"Handling {symbol}:")
    # Get the last database timestamp
    print(f"    Get timestamp:")
    last_stamp = dbh.last_time(symbol=symbol)
    print(f"    Got timestamp!")
    print(f"    Get AV-data:")
    # Get the AV data
    datapoints = av.consume(symbol, last_datestamp=last_stamp)
    print(f"    Got AV-data!")
    print(f"    Save data:")
    # Save the datapoints into the database
    dbh.save_data(datapoints, symbol=symbol)
    print(f"    Saved data!")
    print(f"    Save timestamp:")
    # Save the last data
    dbh.save_lasttime(datapoints, symbol=symbol)
    print(f"    Saved timestamp!")
    print(f"Handled {symbol}!")

def consume_symbols(symbols=[]):
    failed_symbols = []
    for s in symbols:
        try:
            consume_one(symbol=s)
        except ValueError:
            failed_symbols.append(s)
    return failed_symbols
def consume():
    symbols = load_symbols()['symbol']
    failed_symbols = consume_symbols(symbols=symbols)
    try_count = 1
    max_tries = 6
    sleep_time = 20
    while len(failed_symbols) > 1 and try_count < max_tries:
        s_time = sleep_time * try_count
        f_str = ", ".join(failed_symbols)
        print(f"--- Failed: {f_str}, count: {try_count + 1}, waiting: {s_time}s ---")
        sleep(s_time)
        failed_symbols = consume_symbols(symbols=failed_symbols)
        try_count += 1

if __name__ == "__main__":
    consume()


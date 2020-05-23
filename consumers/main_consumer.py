import pandas as pd
import alphavantage as av
import database_handler as dbh

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

def consume():
    symbols = load_symbols()
    for s in symbols['symbol']:
        consume_one(symbol=s)

if __name__ == "__main__":
    consume()


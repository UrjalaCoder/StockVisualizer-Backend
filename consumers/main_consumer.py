import pandas as pd
import alphavantage as av
import database_handler as dbh

# Get the symbols
def load_symbols():
    symbol_df = pd.read_csv("./consumers/symbols.csv", index_col=False)
    return symbol_df

# Get the data for one symbol and add them to the database
def consume_one(symbol="MSFT"):
    # Get the last database timestamp
    last_stamp = dbh.last_time(symbol=symbol)
    
    # Get the AV data
    datapoints = av.consume(symbol, last_datestamp=last_stamp)

    # Save the datapoints into the database
    dbh.save_data(datapoints, symbol=symbol)

    # Save the last data
    dbh.save_lasttime(datapoints, symbol=symbol)

def consume():
    symbols = load_symbols()
    for s in symbols['symbol']:
        consume_one(symbol=s)

if __name__ == "__main__":
    consume()


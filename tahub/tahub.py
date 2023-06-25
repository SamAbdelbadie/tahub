# ------------- IMPORTS -------------
import requests
import pandas as pd
import time
import yfinance as yf
from pandas_datareader import data as pdr



# ------------- FUNCTIONS -------------
# ============= GENERAL =============
def get_binance_historical_data(symbol, interval, start_time, end_time):
    url = "https://api.binance.us/api/v3/klines"
    limit = 1000

    def fetch_data(params):
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception("Failed to retrieve data from Binance API")
        return response.json()

    def process_data(data):
        temp_df = pd.DataFrame(data, columns=[
            'datetime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume',
            'numberOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'
        ])
        temp_df.drop(columns=['ignore'], inplace=True)
        temp_df['datetime'] = pd.to_datetime(temp_df['datetime'], unit='ms')
        temp_df['closeTime'] = pd.to_datetime(temp_df['closeTime'], unit='ms')
        temp_df = temp_df.astype({
            'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
            'volume': 'float', 'quoteAssetVolume': 'float', 'numberOfTrades': 'int',
            'takerBuyBaseAssetVolume': 'float', 'takerBuyQuoteAssetVolume': 'float'
        })
        return temp_df[['datetime', 'open', 'high', 'low', 'close',
                        'volume', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseAssetVolume',
                        'takerBuyQuoteAssetVolume']]

    req_params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    df = pd.DataFrame()

    while True:
        data = fetch_data(req_params)
        if not data:
            break

        temp_df = process_data(data)
        df = pd.concat([df, temp_df], ignore_index=True)

        if len(data) < limit:
            break

        req_params['startTime'] = int(data[-1][0] + 1)
        
        time.sleep(1)

    return df


def get_stock_data(ticker, date_start, date_end, interval):
    """
    df = get_stock_data("ETSC",
                    (date.today() - timedelta(days=365)).strftime('%Y-%m-%d'), 
                    datetime.today().strftime('%Y-%m-%d'),
                    "1h")
    """
    yf.pdr_override() # <== that's all it takes :-)
    df = pdr.get_data_yahoo(ticker, start=date_start, end=date_end,interval='1h')
    df.reset_index(inplace=True)
    df['ticker'] = ticker
    for col in df.columns:
        df.rename(columns={col:col.lower()}, inplace=True)
    
    return df

def compute_lags(number_of_lags, df, column_names):
    for n in range(1, number_of_lags+1):
        for column in column_names:
            df[column + "_lag_"+ str(n) ] = df[column].shift(n)
    return df
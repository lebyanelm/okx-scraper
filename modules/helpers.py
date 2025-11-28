# Dependencies
import datetime
import requests
import time
import pandas as pd
import os
import json
import logging
logging.basicConfig(level = logging.INFO)



def get_recent_klines(symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
    """
    Fetch the most recent OHLCV candlesticks for a given symbol and interval
    from the exchange (Binance Spot or Futures, depending on the endpoint used).

    Parameters
    ----------
    symbol : str
        Trading pair symbol, e.g. "BTCUSDT".
    interval : str
        Candle interval such as "1m", "5m", "1h", "1d".
    limit : int, optional
        Number of candles to fetch (default 1000, maximum allowed by Binance).

    Returns
    -------
    pandas.DataFrame: A DataFrame containing up to `limit` recent candles with columns:
    [
        time_open, open, high, low, close, volume,
        time_close, quote_vol, trades, taker_base,
        taker_quote, ignore
    ]

    - time_open and time_close are converted to datetime (UTC).
    - numerical fields (open/high/low/close/volume) are converted to float.

    Notes
    -----
    - Only retrieves recent candles, not full history.
    - Binance timestamps are returned in UTC.
    - Use WebSockets afterwards for real-time updates.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    data = requests.get(url, params=params).json()
    df = pd.DataFrame(data, columns=[
        "time_open", "open", "high", "low", "close", "volume",
        "time_close", "quote_vol", "trades", "taker_base",
        "taker_quote", "ignore"
    ])
    df["time_open"] = pd.to_datetime(df["time_open"], unit="ms")
    df["time_open"] = df["time_open"] + datetime.timedelta(hours=2)
    df["time_close"] = pd.to_datetime(df["time_close"], unit="ms")
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df


def load_configuration() -> dict:
    """
    Load configuration parameters from `config.json` and return them
    as a Python dictionary.

    Returns
    -------
    dict
        Parsed configuration values.

    Raises
    ------
    FileNotFoundError
        If `config.json` does not exist.
    json.JSONDecodeError
        If the file contains invalid JSON.
    """
    with open("./config.json", "r") as f:
        return json.load(f)
config  = load_configuration()


def load_models() -> dict:
    return dict()


def safe_request(fn, *args, **kwargs) -> any:
    for _ in range(5):
        r = fn(*args, **kwargs)
        try:
            r_json = r.json()
            if r_json['code'] == '0':
                return r
            time.sleep(0.25)
        except:
            return []
    raise []
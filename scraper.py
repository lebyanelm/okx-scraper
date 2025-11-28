import requests
import time
import datetime
import random
import pandas
import logging
import modules.helpers
from sqlalchemy.dialects.mysql import insert
import sql


BASE_URL = "https://www.okx.com"
HISTORICAL_ENDPOINT = f"{BASE_URL}/api/v5/market/history-candles"


SYMBOLS = modules.helpers.config["SYMBOLS"]
INTERVALS = modules.helpers.config["INTERVALS"]
PREVIOUS_END = None


DEFAULT_PARAMS = dict(
    limit = 300
)
DELTA = datetime.timedelta(minutes = DEFAULT_PARAMS["limit"])


session = requests.Session()


broker = sql.broker.get_broker_by_name("OKX")
if not broker:
    sql.broker.create_broker("okx.com", "OKX")


for SYMBOL in SYMBOLS:
    print(f"------------------- {SYMBOL} -----------")

    """ Check if an existing asset exists, if not create one. """
    asset = sql.asset.get_asset_by_symbol(SYMBOL, "okx.com")
    if not asset:
        sql.asset.create_asset(SYMBOL, "", "okx.com", "crypto")
    DEFAULT_PARAMS["instId"] = SYMBOL

    for INTERVAL in INTERVALS:
        print(f"---------------------- {INTERVAL} ----------------------")
        DEFAULT_PARAMS["bar"] = INTERVAL
        CANDLES_COUNT = 0

        """ Incase the script was stopped before completing, try to continue where left off. """
        oldest_candle = sql.candle.get_oldest_candle(
            SYMBOL,
            INTERVAL
        )

        """ Prepare the default symbol starting parameters. """
        if oldest_candle:
            candle_date = pandas.to_datetime(oldest_candle.timestamp)
            logging.info(f"Found oldest candlestick: {candle_date}")
            DEFAULT_PARAMS["after"] = int(candle_date.timestamp() * 1000)
        
        while True:
            try:
                """ Request the data. """
                data_response = modules.helpers.safe_request(session.get, HISTORICAL_ENDPOINT, params = DEFAULT_PARAMS)
                data = data_response.json().get("data", [])

                """ Extract the required features. """
                candlesticks = []
                for candle in data:
                    candlesticks.append({
                        "timestamp": datetime.datetime.fromtimestamp(int(candle[0]) / 1000),
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[7]),
                        "asset_id": SYMBOL,
                        "period": INTERVAL
                    })
                    
                candles_length = len(candlesticks)
                CANDLES_COUNT += candles_length
                logging.info(f"Current total candlesticks for session: {CANDLES_COUNT}.")
                if candles_length:
                    if PREVIOUS_END:
                        logging.info(f"Date delta from past candles: {PREVIOUS_END - candlesticks[-1]['timestamp']}")
                    PREVIOUS_END = candlesticks[-1]["timestamp"]
                    logging.info(f"Received {len(candlesticks)} candles "
                        f"Symbol=[{SYMBOL} | {INTERVAL}] Date={PREVIOUS_END}")
                    
                    """ Store in DB. """
                    stmt = insert(sql.orms.candle.Candle).values(candlesticks)
                    update_cols = {c.name: c for c in stmt.inserted if c.name not in ['id']}
                    upsert_stmt = stmt.on_duplicate_key_update(**update_cols)
                    with sql.session.begin():
                        sql.session.execute(upsert_stmt)
                    sql.session.commit()

                    """ Pagination (move backward in time). """
                    DEFAULT_PARAMS["after"] = int(PREVIOUS_END.timestamp() * 1000)

                    """ Sleep to avoid ip-blocks. """
                    time.sleep(random.uniform(0.12, 0.20))
                else:
                    """ Data might have reached the end. """
                    logging.error(data_response.text)
                    if DEFAULT_PARAMS.get("after"):
                        del DEFAULT_PARAMS["after"]
                    break
            except Exception as e:
                logging.error(e)
                sql.session.rollback()
                continue
            print()
        print()
    print()
logging.info("Scraping data completed.")
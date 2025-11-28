# CONNECTION
import sql.connection
session = sql.connection.session
Base = sql.connection.Base
engine = sql.connection.engine

import logging


# ORMS
import sql.orms.asset
asset = sql.orms.asset

import sql.orms.broker
broker = sql.orms.broker

import sql.orms.candle
candle = sql.orms.candle

# CREATE NEW TABLES
Base.metadata.create_all(engine)
logging.info("Database has succesfully connected.")
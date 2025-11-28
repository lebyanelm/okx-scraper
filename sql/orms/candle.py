import sql.connection
from datetime import datetime, timezone
from sqlalchemy import (
     Column, Integer, String, Numeric, DECIMAL, DateTime,
    ForeignKey, UniqueConstraint, func, desc, asc
)
from sqlalchemy.orm import relationship
from sql.orms.asset import get_asset_by_symbol


class Candle(sql.connection.Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(String(50), ForeignKey("assets.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(DECIMAL(18, 6))
    high = Column(DECIMAL(18, 6))
    low = Column(DECIMAL(18, 6))
    close = Column(DECIMAL(18, 6))
    volume = Column(DECIMAL(38, 12))
    period = Column(String(10), default="1m")

    asset = relationship("Asset", back_populates="candles")

    __table_args__ = (UniqueConstraint("asset_id", "timestamp", "period", name="uq_asset_time"),)


def create_candle(symbol: str, timestamp: datetime, open_, high, low, close, period="60", broker_name=None):
    if timestamp.tzinfo:
        timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)

    asset = get_asset_by_symbol(symbol, broker_name)
    if not asset:
        raise ValueError(f"Asset {symbol} not found. Create it first.")

    existing = sql.connection.session.query(Candle).filter_by(
        asset_id=asset.id,
        timestamp=timestamp,
        period=str(period)
    ).first()

    if existing:
        existing.open, existing.high, existing.low, existing.close = open_, high, low, close
        sql.connection.session.commit()
        return existing

    candle = Candle(
        asset_id=asset.id,
        timestamp=timestamp,
        open=open_,
        high=high,
        low=low,
        close=close,
        period=str(period)
    )
    """
    [
        0  open time (ms)
        1  open
        2  high
        3  low
        4  close
        5  volume (base asset)
        6  close time (ms)
        7  quote asset volume
        8  number of trades
        9  taker buy base volume
        10  taker buy quote volume
        11  ignored
    ]

    """
    sql.connection.session.add(candle)
    sql.connection.session.commit()
    return candle


def get_candle_by_id(candle_id: int):
    return sql.connection.session.query(Candle).filter_by(id=candle_id).first()


def get_candles_by_symbol(symbol: str, start_date=None, end_date=None, period="60", broker_name=None):
    asset = get_asset_by_symbol(symbol, broker_name)
    if not asset:
        return []
    q = sql.connection.session.query(Candle).filter_by(asset_id=asset.id, period=str(period))
    if start_date:
        q = q.filter(Candle.timestamp >= start_date)
    if end_date:
        q = q.filter(Candle.timestamp <= end_date)
    return q.order_by(Candle.timestamp.asc()).all()


def count_candles_by_asset(symbol: str, broker: str) -> int:
    asset = get_asset_by_symbol(symbol, broker)
    if asset:
        return sql.connection.session.query(func.count(Candle.id))\
            .filter(Candle.asset_id == symbol).scalar()
    return 0


def get_oldest_candle(asset_id: str = None, period: str | int = None) -> Candle:
    q = sql.connection.session.query(Candle).order_by(asc(Candle.timestamp))
    if asset_id:
        q = q.filter(Candle.asset_id == asset_id)
    if period:
        q = q.filter(Candle.period == str(period))
    return q.first()


def get_latest_candle(asset_id: str = None, period: str | int = None):
    q = sql.connection.session.query(Candle).order_by(desc(Candle.timestamp))
    if asset_id:
        q = q.filter(Candle.asset_id == asset_id)
    if period:
        q = q.filter(Candle.period == str(period))
    return q.first()


def update_candle(candle_id: int, **kwargs):
    candle = get_candle_by_id(candle_id)
    if not candle:
        return None
    for k, v in kwargs.items():
        if hasattr(candle, k):
            setattr(candle, k, v)
    sql.connection.session.commit()
    return candle


def delete_candle(candle_id: int):
    candle = get_candle_by_id(candle_id)
    if candle:
        sql.connection.session.delete(candle)
        sql.connection.session.commit()
        return True
    return False


def delete_candles_by_symbol(symbol: str, broker_name=None):
    asset = get_asset_by_symbol(symbol, broker_name)
    if not asset:
        return 0
    count = sql.connection.session.query(Candle).filter_by(asset_id=asset.id).delete()
    sql.connection.session.commit()
    return count
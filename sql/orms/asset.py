import sql.connection
from sql.orms.broker import (
    create_broker, get_broker_by_id, get_broker_by_name
)
from sqlalchemy import (
     Column, Integer, String,
    ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship


class Asset(sql.connection.Base):
    __tablename__ = "assets"
    id = Column(String(50), primary_key=True)  # symbol
    broker_id = Column(String(50), ForeignKey("brokers.id"), nullable=False)
    asset_name = Column(String(50), nullable=False)
    asset_type = Column(String(50), nullable=True)
    candle_count = Column(Integer(), nullable=True)
    timeframes = Column(String(100), nullable=True)
    broker = relationship("Broker", back_populates="assets")
    candles = relationship("Candle", back_populates="asset", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("broker_id", "id", name="uq_broker_id"),)


def create_asset(id: str, name, broker_id: str, asset_type: str = None) -> Asset:
    broker = get_broker_by_id(broker_id)
    existing = sql.connection.session.query(Asset).filter_by(id=id, broker_id=broker.id).first()
    if existing:
        return existing
    asset = Asset(id=id, asset_name=name, broker_id=broker.id, asset_type=asset_type)
    sql.connection.session.add(asset)
    sql.connection.session.commit()
    return asset


def get_assets_by_type(asset_type: str):
    return sql.connection.session.query(Asset).filter_by(asset_type=asset_type).all()


def get_asset_by_symbol(symbol: str, broker_id: str = None):
    q = sql.connection.session.query(Asset).filter_by(id=symbol)
    if broker_id:
        broker = get_broker_by_id(broker_id)
        if broker:
            q = q.filter_by(broker_id=broker.id)
    return q.first()


def list_assets(broker_name: str = None):
    q = sql.connection.session.query(Asset)
    if broker_name:
        broker = get_broker_by_name(broker_name)
        if broker:
            q = q.filter_by(broker_id=broker.id)
    return q.all()


def update_asset_symbol(symbol: str, new_symbol: str):
    asset = get_asset_by_symbol(symbol)
    if asset:
        asset.id = new_symbol
        sql.connection.session.commit()
    return asset


def delete_asset(symbol: str):
    asset = get_asset_by_symbol(symbol)
    if asset:
        sql.connection.session.delete(asset)
        sql.connection.session.commit()
        return True
    return False
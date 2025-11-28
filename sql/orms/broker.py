import sql.connection
from sqlalchemy import (
    Column, String
)
from sqlalchemy.orm import relationship


class Broker(sql.connection.Base):
    __tablename__ = "brokers"
    id = Column(String(50), primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    assets = relationship("Asset", back_populates="broker", cascade="all, delete-orphan")


def create_broker(id: str, name: str):
    existing = sql.connection.session.query(Broker).filter_by(id=id).first()
    if existing:
        return existing
    broker = Broker(id=id, name=name)
    sql.connection.session.add(broker)
    sql.connection.session.commit()
    return broker


def get_broker_by_id(id: str):
    return sql.connection.session.query(Broker).filter_by(id=id).first()


def get_broker_by_name(name: str):
    return sql.connection.session.query(Broker).filter_by(name=name).first()


def list_brokers():
    return sql.connection.session.query(Broker).all()


def update_broker(id: str, new_name: str):
    broker = sql.connection.get_broker_by_id(id)
    if broker:
        broker.name = new_name
        sql.connection.session.commit()
    return broker


def delete_broker(id: str):
    broker = sql.connection.get_broker_by_id(id)
    if broker:
        sql.connection.session.delete(broker)
        sql.connection.session.commit()
        return True
    return False
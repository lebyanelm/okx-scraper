import modules.helpers
from sqlalchemy import (
    create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker



# ---------------------------- CONNECTION -------------------------
DB_CONN = (
    f"mysql+pymysql://{modules.helpers.config["SEC_USERNAME"]}:{modules.helpers.config["SEC_MASTER_PD"]}@"
    f"{modules.helpers.config["SEC_MASTER_ENDPOINT"]}:3306/{modules.helpers.config["SEC_MASTER_NAME"]}"
)
Base = declarative_base()
engine = create_engine(DB_CONN, echo=False, isolation_level="READ COMMITTED")
Session = sessionmaker(bind=engine)
session = Session()
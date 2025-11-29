import os
import modules.helpers
from sqlalchemy import (
    create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker



# ---------------------------- CONNECTION -------------------------
print(os.environ)
DB_CONN = (
    f"mysql+pymysql://{os.environ["SEC_USERNAME"]}:{os.environ["SEC_MASTER_PD"]}@"
    f"{os.environ["SEC_MASTER_ENDPOINT"]}:3306/{os.environ["SEC_MASTER_NAME"]}"
)
Base = declarative_base()
engine = create_engine(DB_CONN, echo=False, isolation_level="READ COMMITTED")
Session = sessionmaker(bind=engine)
session = Session()
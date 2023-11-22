import os
import datetime
from typing import Optional

import sqlalchemy
import urllib.parse
from src.secrets_manager import get_secret

_engine: Optional[sqlalchemy.engine.Engine] = None
_engine_init_time: Optional[datetime.datetime] = None


db_username = get_secret("SQL_USERNAME")
db_passwd = get_secret("SQL_PASSWORD")
if db_passwd:
    db_passwd = urllib.parse.quote(db_passwd)
db_hostname = get_secret("SQL_HOSTNAME")
db_schema = get_secret("SQL_SCHEMA")

def get_engine() -> sqlalchemy.engine.Engine:
    global _engine
    global _engine_init_time

    if _engine is None:
        url = f"mysql+pymysql://{db_username}:{db_passwd}@{db_hostname}/{db_schema}"
        _engine = sqlalchemy.create_engine(url)
        _engine_init_time = datetime.datetime.now()

    return _engine

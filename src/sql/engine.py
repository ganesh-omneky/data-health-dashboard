import datetime
import os
import urllib.parse
from time import sleep
from typing import Optional

import sqlalchemy
from sshtunnel import SSHTunnelForwarder

from src.secrets_manager import get_secret

db_hostname = get_secret("DB_HOST")

tunnel_forwarder: Optional[SSHTunnelForwarder] = None
ssh_host = get_secret("SSH_HOST")
ssh_user = get_secret("SSH_USER")
ssh_pkey = get_secret("SSH_PKEY")
tunnel_forwarder = SSHTunnelForwarder(
    ssh_host,
    ssh_username=ssh_user,
    ssh_pkey=ssh_pkey,
    remote_bind_address=(db_hostname, 3306),
)

tunnel_forwarder.start()

_engine = None


def get_engine() -> sqlalchemy.engine.Engine:
    db_username = get_secret("DB_USER")
    db_passwd = get_secret("DB_PASSWORD")
    if db_passwd:
        db_passwd = urllib.parse.quote(db_passwd)
    db_schema = get_secret("DB_NAME")

    global tunnel_forwarder
    global _engine

    local_to_prod = os.environ.get("LOCAL_TO_PROD") == "1"

    if not _engine:
        if local_to_prod:
            while not tunnel_forwarder.is_active or not tunnel_forwarder.is_alive:
                print("Waiting for tunnel to be active and alive...")
                sleep(1)
            local_port = tunnel_forwarder.local_bind_port
            url = f"mysql+pymysql://{db_username}:{db_passwd}@localhost:{local_port}/{db_schema}"
        else:
            url = f"mysql+pymysql://{db_username}:{db_passwd}@{db_hostname}/{db_schema}"

        _engine = sqlalchemy.create_engine(url)

    return _engine

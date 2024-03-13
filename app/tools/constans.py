from .db import DatabaseConnection
from dotenv import load_dotenv
import os

load_dotenv(".env")

# DATA BASE P
paramsp = {
    "host": os.environ.get("POSTGRES_HOST_P"),
    "database": os.environ.get("POSTGRES_DB_P"),
    "user": os.environ.get("POSTGRES_USER_P"),
    "password": os.environ.get("POSTGRES_PASSWORD_P"),
    "port": os.environ.get("POSTGRES_PORT_P") 
}

# DATA BASE L
paramsl = {
    "host": os.environ.get("POSTGRES_HOST") ,
    "database": os.environ.get("POSTGRES_DB") ,
    "user": os.environ.get("POSTGRES_USER") ,
    "password": os.environ.get("POSTGRES_PASSWORD") ,
    "port": os.environ.get("POSTGRES_PORT") 
}


# AWS
BUCKET = os.environ.get("AWS_BUCKET_NAME")

# PATHS
TRASH_PATH = "trash"

# STATUS INPUTS
"""Esto es una cochinada, pero que se le va ha hacer"""
conn = DatabaseConnection(**paramsp)
conn.connect()

STATUS = {
    id_status: msm
    for id_status, msm in conn.execute('select * from public."statusInput"')
}

conn.close()
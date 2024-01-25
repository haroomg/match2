from dotenv import load_dotenv
import os

load_dotenv(".env")

paramsp = {
    "host": os.environ.get("POSTGRES_HOST_P"),
    "database": os.environ.get("POSTGRES_DB_P"),
    "user": os.environ.get("POSTGRES_USER_P"),
    "password": os.environ.get("POSTGRES_PASSWORD_P"),
    "port": os.environ.get("POSTGRES_PORT_P") 
}

paramsl = {
    "host": os.environ.get("POSTGRES_HOST") ,
    "database": os.environ.get("POSTGRES_DB") ,
    "user": os.environ.get("POSTGRES_USER") ,
    "password": os.environ.get("POSTGRES_PASSWORD") ,
    "port": os.environ.get("POSTGRES_PORT") 
}

DATA_PATH = "trash/data"
DB_PATH = "trash/db"
FASTDUP_PATH = "trash/fastdup"
IMG_PATH = "trash/img" 
S3_PATH = "trash/s3"
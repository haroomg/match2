from dotenv import load_dotenv
import os

load_dotenv()

# debig
DEBUG = int(os.environ.get("DEBUG"))

# database
if DEBUG: # if in test
    
    DATABASES = {
        "host": os.environ.get("POSTGRES_HOST_T"),
        "database": os.environ.get("POSTGRES_DB_T"),
        "user": os.environ.get("POSTGRES_USER_T"),
        "password": os.environ.get("POSTGRES_PASSWORD_T"),
        "port": os.environ.get("POSTGRES_PORT_T") 
    }

else:

    DATABASES = {
        "host": os.environ.get("POSTGRES_HOST_P"),
        "database": os.environ.get("POSTGRES_DB_P"),
        "user": os.environ.get("POSTGRES_USER_P"),
        "password": os.environ.get("POSTGRES_PASSWORD_P"),
        "port": os.environ.get("POSTGRES_PORT_P") 
    }

# aws
AWS = {
    "s3":{
        "access_key_id":os.environ.get("AWS_ACCESS_KEY_ID"),
        "secret_access_key":os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "bucket_name":os.environ.get("AWS_BUCKET_NAME"),
        "region":os.environ.get("AWS_REGION"),
        "output_format":os.environ.get("AWS_OUTPUT_FORMAT")
    },
    "sc2":{}
}

# fastdup
FASTDUP = {
    "host":os.environ.get("FASTDUP_HOST"),
    "port":os.environ.get("FASTDUP_PORT")
}

# path dirs
TRASH = "trash"
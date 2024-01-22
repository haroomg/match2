from fastapi import HTTPException
from .db import DatabaseConnection
from dotenv import load_dotenv
from .s3 import client, bucket
from .local_paths import *
from .functions import *
import shutil
import ijson
import os

load_dotenv(".env")

paramsp = {
    "host": os.environ.get("POSTGRES_HOST_P"),
    "database": os.environ.get("POSTGRES_DB_P"),
    "user": os.environ.get("POSTGRES_DB_P"),
    "password": os.environ.get("POSTGRES_PASSWORD_P"),
    "port": os.environ.get("POSTGRES_PORT_P") 
}

paramsl = {
    "host": os.environ.get("POSTGRES_HOST") ,
    "database": os.environ.get("POSTGRES_DB") ,
    "user": os.environ.get("POSTGRES_DB") ,
    "password": os.environ.get("POSTGRES_PASSWORD") ,
    "port": os.environ.get("POSTGRES_PORT") 
}

connp = DatabaseConnection(**paramsp)
connl = DatabaseConnection(**paramsl)


def load(request_id: str = None) -> dict:
    
    connp.connect()
    connl.connect()
    
    # validamos que existe el request_id
    query = f'SELECT "filesPath" FROM "Request" WHERE id = %s'
    connp.execute(query, (request_id,))
    
    result =  connp.result.fetchone()
    
    if result:
        
        request_table = "request_" + request_id.replace("-", "_")
        
        files_path = result[0]
        
        # validamos que el esquema no exista en la base de datos
        query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s"
        connl.execute(query, (request_table,))
        
        result = connl.result.fetchone()
        
        if not result:
            
            query = f'CREATE SCHEMA {request_table}'
            connl.execute(query)
            connl.commit()
            
            connp.close()
            
        else:
            raise HTTPException(status_code=402, detail=f"El {request_id} no existe o esta mal escrito.")
        
    else:
        raise HTTPException(status_code=402, detail=f"El {request_id} ya esta registrado.")
    
    # descargamos los archivos que vamos a subir en la db 
    local_path = os.path.join(S3_PATH, request_table)
    os.mkdir(local_path)
    
    for file in ["origin.json", "alternative.json"]:
        
        local_file = os.path.join(local_file, file)
        s3_path = os.path.join(files_path, file)
        client.download_file(bucket, s3_path, local_file)
        
        #3
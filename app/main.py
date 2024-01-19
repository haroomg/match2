from fastapi import FastAPI, HTTPException
from .shema import Load_reques
from dotenv import load_dotenv
import psycopg2
import shutil
import boto3
import os

app = FastAPI()
load_dotenv()

# Crear una instancia del cliente de S3
s3_client = boto3.client(
    's3', 
    aws_access_key_id= os.environ.get("AWS_ACCESS_KEY_ID"), 
    aws_secret_access_key= os.environ.get("AWS_SECRET_ACCESS_KEY")
)
# nombre del bucket donde vamos a sacar la informacion
bucket = os.environ.get("AWS_BUCKET_NAME")

@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id: str = request.request_id
    
    # creamos la coneccion a la bd de hydra
    db_params_p = {
        "host": os.environ.get("POSTGRES_HOST_P"),
        "database": os.environ.get("POSTGRES_DB_P"),
        "user": os.environ.get("POSTGRES_DB_P"),
        "password": os.environ.get("POSTGRES_PASSWORD_P"),
        "port": os.environ.get("POSTGRES_PORT_P") 
    }
    conn_p = psycopg2.connect(**db_params_p)
    cursor_p = conn_p.cursor()
    cursor_p.execute(f'SELECT "filesPath" FROM "Request" WHERE id = %s', (request_id,))
    
    result = cursor_p.fetchone()
    
    # verificamos que exista el request_id
    if result == None:
        raise HTTPException(status_code=402, detail=f"El {request_id} no existe o esta mal escrito.")
    else:
        files_path = result[0]
        cursor_p.close()
        conn_p.close()
    
    # creamos la coneccion a la bd local
    db_params_l = {
        "host": os.environ.get("POSTGRES_HOST") ,
        "database": os.environ.get("POSTGRES_DB") ,
        "user": os.environ.get("POSTGRES_DB") ,
        "password": os.environ.get("POSTGRES_PASSWORD") ,
        "port": os.environ.get("POSTGRES_PORT") 
    }
    conn_l = psycopg2.connect(**db_params_l)
    cursor_l = conn_l.cursor()
    
    # creamos un schema con el nombre del request_id
    cursor_l.execute(f'CREATE SCHEMA request_{request_id.replace("-", "_")}')
    conn_l.commit()
    
    # descargamos los archivos origin y alternative del files_path obtenido
    local_path = os.path.join("trash/s3/", request_id)
    os.makedirs(local_path)
    
    origin = "origin.json"
    alternative =  "alternative.json"
    
    for filename in [origin, alternative]:
        
        local_file = os.path.join(local_path, filename)
        s3_path = os.path.join(files_path, filename)
        s3_client.download_file(bucket, s3_path, local_file)
    
    return {
        "result": files_path
    }
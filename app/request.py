from fastapi import HTTPException
from .tools.s3 import client, bucket
from .tools.functions import *
from .tools.constans import *
import shutil
import os



def load(request_id: str = None, connp = None, connl = None) -> dict:
    
    connp.connect()
    connl.connect()
    
    # validamos que existe el request_id
    query = f'SELECT "filesPath" FROM "Request" WHERE id = %s'
    connp.execute(query, (request_id,))
    
    result =  connp.result.fetchone()
    
    if result != None:
        
        request_shema = "request_" + request_id.replace("-", "_")
        
        files_path = result[0]
        
        # validamos que el esquema no exista en la base de datos
        query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s"
        connl.execute(query, (request_shema,))
        
        result = connl.result.fetchone()
        if result == None:
            
            query = f'CREATE SCHEMA {request_shema}'
            connl.execute(query)
            connl.commit()
            
            connp.close()
            
        else:
            connp.close()
            raise HTTPException(status_code=402, detail=f"El {request_id} ya esta registrado como {request_shema}.")
        
    else:
        connp.close()
        raise HTTPException(status_code=402, detail=f"El {request_id} no existe o esta mal escrito.")
        
    
    # descargamos los archivos que vamos a subir en la db 
    local_path = os.path.join(S3_PATH, request_shema)
    os.mkdir(local_path)
    
    tables_name = []
    
    for file in ["origin.json", "alternative.json"]:
        
        local_file = os.path.join(local_path, file)
        s3_path = os.path.join(files_path, file)
        client.download_file(bucket, s3_path, local_file)
        name = file.split(".")[0]
        
        create_load_data(
            shema_name= request_shema,
            table_name= name,
            path_file= local_file,
            conn= connl
        )
        
        tables_name.append(name)
        
        os.remove(local_file)
    shutil.rmtree(local_path)
    
    # creamos la tabla inputs
    query = f"CREATE TABLE IF NOT EXISTS {request_shema}.inputs(id SERIAL PRIMARY KEY, input JSONB, date_create timestamp DEFAULT CURRENT_TIMESTAMP)"
    connl.execute(query)
    connl.commit()
    
    # creamos la tabla machings
    query = f"""CREATE TABLE IF NOT EXISTS {request_shema}.matchings(
        id SERIAL PRIMARY KEY, 
        input_id INTEGER REFERENCES {request_shema}.inputs (id), 
        origin_id INTEGER REFERENCES {request_shema}.{tables_name[0]} (id),
        alternative_id INTEGER REFERENCES {request_shema}.{tables_name[1]} (id),
        similarity FLOAT,
        date_create timestamp DEFAULT CURRENT_TIMESTAMP)
        """
    connl.execute(query)
    
    # guardamos los cambios efectuados
    connl.commit()
    connl.close()
    
    return {
        "msm": f"Se aca de crear y subir la data en el shema: {request_shema}",
        "schema_name": request_shema,
        "tables_created": ["origin", "alternative", "inputs", "matchings"]
    }
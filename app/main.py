from .db import DatabaseConnection
from dotenv import load_dotenv
from .functions import search
from fastapi import FastAPI, HTTPException
from .request import load
from .match import image
from .constans import *
from .shema import *
import sqlite3
import shutil
import os

app = FastAPI()
connp = DatabaseConnection(**paramsp)
connl = DatabaseConnection(**paramsl)


@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id: str = request.request_id
    
    return load(request_id)


@app.post("/match/img")
def match_img(request: Match_img):
    
    request_id: str = request.request_id
    input: dict = request.input
    s3_path_img_origin: str = request.s3_path_img_origin
    s3_path_img_alternative: str = request.s3_path_img_alternative
    
    input_origin: dict = input["origin"]
    input_alternative: dict = input["alternative"]
    
    connl.connect()
    
    search_origin = search(request_id= request_id, comlumns= ["id","product_images"], input= input_origin, table_name= "origin", conn= connl)
    search_alternative = search(request_id=request_id, comlumns=["id","product_images"], input= input_alternative, table_name= "alternative", conn= connl)

    # validamos que las busqueda tengan data 
    if not search_origin or not search_alternative:
        msm_error = {
            "msm": "Los inputs proporcionados no traen información, revisa los parámetros de búsqueda"
        }
        
        if not search_origin:
            msm_error["origin"] = "No se encontró información con los parámetros del  origin"
        if not search_alternative:
            msm_error["alternative."] = "No se encontró información con los parámetros del  alternative."
        
        if len(msm_error):
            raise HTTPException(status_code=402, detail=msm_error)
    
    request_name = "request_" + request_id.replace("-", "_")
    query = f"INSERT INTO {request_name}.inputs (input) VALUES (%s) RETURNING id"
    params = (str(input).replace("'", "\"").replace("None", "null"),)
    connl.execute(query, str(input))
    connl.commit()
    input_id = connl.result.fetchone()[0]
    
    #creamos la carpeta donde vamos a guardar los resultados de fastdup
    fastdup_dir = os.path.join(FASTDUP_PATH, request_name)
    image_dir = os.path.join(IMG_PATH, request_name)
    db_name = fastdup_dir + ".sqlite"
    filename_images = os.path.join(fastdup_dir, "path_images.txt") 
    
    # creamos las carpetas
    os.makedirs(fastdup_dir)
    os.makedirs(image_dir)
    
    with open(filename_images, "w", encoding="ut-8") as file:
        with sqlite3.connect(db_name) as conn_lite:
            
            for name in ["origin", "alternative"]:
                conn_lite.execute(F"CREATE TABLE IF NOT EXISTS {name}(id INTEGER, file_name VARCHAR(256))")
            
            conn_lite.commit()
            
            query = "INCERTO INTO {name}(id, file_name)  VALUES (?, ?)"
            
            s3_path = "s3://" + BUCKET + s3_path_img_origin
            
            for data in search_origin:
                
                id_ = data[0]
                files_name = data[1]
                
                for name in files_name:
                    conn_lite.execute(query.format(table_name="origin"), (id_, name,))
                    path = os.path.join(s3_path, name)
                    file.write(path+"\n")
            
            s3_path = "s3://" + BUCKET + s3_path_img_alternative
            
            for data in search_alternative:
                
                id_ = data[0]
                files_name = data[1]
                
                for name in files_name:
                    conn_lite.execute(query.format(table_name="alternative"), (id_, name,))
                    path = os.path.join(s3_path, name)
                    file.write(path+"\n")
                    
            conn_lite.commit()
        conn_lite.close()
        
    #3 hay que ver el proceso de descargar las imagenes
        
    # similarity = image(filename_images)
    
    # # borramos la carpeta con las imagenes que descargamos
    # shutil.rmtree(image_dir)
    
    # # cambiamos el la direccion de las imagenes para que solo sea el nombre del archivo
    # for col_name in ["filename_from", "filename_to"]: 
    #     similarity[col_name] = similarity[col_name].apply(lambda x : os.path.basename(x))
    
    # with sqlite3.connect(db_name) as conn_lite:
        
    #     query = "SELECT id FROM {table_name} WHERE file_name = {file_name}"
    #     query2 = F"INSERT INTO {request_name}.matches(input_id, origin_id, alternative_id, similarity) VALUES(%s, %s, %s, %s)"
        
    #     for i, row in similarity.iterrows():

    #         filename_from = row["filename_from"]
    #         origin_id = conn_lite.execute(query.format("origin", filename_from)).fetchone()

    #         if origin_id:

    #             filename_to = row["filename_to"]
    #             alternative_id = conn_lite.execute(query.format("alternative", filename_to)).fetchone()
                
    #             if not alternative_id:
    #                 continue
    #             else:
    #                 params = (input_id, origin_id[0], alternative_id[0], row["distance"])
    #                 connl.execute(query2, params= params)
                    
    # conn_lite.close()
    
    # connl.commit()
    # connl.close()
    
    # shutil.rmtree(image_dir)
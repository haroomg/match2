from .tools.functions import search_db, download_images, add_metadata
from .tools.db import DatabaseConnection
from fastapi import HTTPException
from .tools.constans import *
from uuid import uuid4
import pandas as pd
import fastdup 
import sqlite3
import shutil
import json
import os

get_request_name = lambda request : "request_" + request.replace("-", "_")


def fd(path_imgs: str = None, bring_invalidImages: bool = False) -> pd.DataFrame:

    fastdup_dir = os.path.dirname(path_imgs)
    fd = fastdup.create(fastdup_dir)
    
    # revisamos las imagenes con fastdup
    fd.run(path_imgs, threshold= 0.5, overwrite= True, high_accuracy= True)

    similarity = fd.similarity()

    if bring_invalidImages:

        # pedimos las imagenes que son invalidas
        invalid_img: list = fd.invalid_instances()["filename"].to_list()

        return similarity, invalid_img
    
    else:
        return similarity


def match_img(
        request_id: str = None, 
        input: dict = None, 
        s3_path_img_origin: str = None, 
        s3_path_img_alternative: str = None
        ) -> dict:
    
    connl = DatabaseConnection(connect= True, **paramsl)
    connp = DatabaseConnection(connect= True, **paramsp)
    
    request_name = get_request_name(request_id)

    # validamos que el input no exista en caso contrario notificamos que ya existe
    query = f"SELECT * FROM {request_name}.inputs WHERE input = %s"
    params = (json.dumps(input),)
    connl.execute(query, params)
    result = connl.result.fetchone()

    if result is None:
        exist = True
    elif result[3] == False:
        exist = True
    else:
        exist = False
    
    # si la consulta no retorna un None quiere decir que ya existe
    # pero si retorna informacion y su status es false segimos

    if not exist:

        msm = {
            "msm":"El input ya existe",
            "input_id": result[0],
            "input": result[1],
            "date_create": str(result[2])
        }

        raise HTTPException(status_code=409, detail=msm)
    
    else: 
    
        search_origin = search_db(schema_name= request_name, comlumns= ["id","products"], parameter= input["origin"], table_name= "origin")
        search_alternative = search_db(schema_name= request_name, comlumns=["id","products"], parameter= input["alternative"], table_name= "alternative")

        print(search_origin)
        print(search_alternative)
    #     # validamos que las busqueda tengan data 
    #     if not search_origin or not search_alternative:
    #         msm_error = {
    #             "msm": "Los inputs proporcionados no traen información, revisa los parámetros de búsqueda"
    #         }
            
    #         if not search_origin:
    #             msm_error["origin"] = "No se encontró información con los parámetros del origin"
    #         if not search_alternative:
    #             msm_error["alternative."] = "No se encontró información con los parámetros del alternative."
            
    #         if len(msm_error):
    #             connl.close()
    #             raise HTTPException(status_code=402, detail=msm_error)

    #     directory_path = {
    #         "root_address": os.path.join(TRASH_PATH, str(uuid4)),
    #     }

    #     if result == None:
    #         query = f"INSERT INTO {request_name}.inputs (input, status) VALUES (%s, %s) RETURNING id"
    #         connl.execute(query, (json.dumps(input), True), commit= True)
    #         input_id = connl.result.fetchone()[0]

    #     elif result[3] == False:
    #         input_id = result[0]

    #         query = f"UPDATE {request_name}.inputs SET status=true WHERE id= %s"
    #         connl.execute(query, (input_id, ))

    #     directory_path["img"] = os.path.join(directory_path["root_address"], "img")
    #     directory_path["db"] = os.path.join(directory_path["root_address"], "db")
    #     directory_path["fastdup"] = os.path.join(directory_path["root_address"], "fastdup")

    #     # creamos la carpeta donde vamos a guardar los resultados de fastdup, las imagenes
    #     db_name =  os.path.join(directory_path["db"] , (request_name + ".sqlite"))
    #     filename_images = os.path.join(directory_path["fastdup"], "path_images.txt") 

    #     for path in directory_path.values():
    #         os.mkdir(path)

        
    # with open(filename_images, "w", encoding="utf-8") as file:
    #     # creamos una db local para guardar la relacion entre la imagene y el id

    #     with sqlite3.connect(db_name) as conn_lite:
            
    #         for name in ["origin", "alternative"]:
    #             conn_lite.execute(F"CREATE TABLE IF NOT EXISTS {name}(id INTEGER, file_name VARCHAR(1024))")
            
    #         conn_lite.commit()

    #         for search, table_name, path_s3 in zip([search_origin, search_alternative], ["origin", "alternative"], [s3_path_img_origin, s3_path_img_alternative]):
                
    #             query = f"INSERT INTO {table_name}(id, file_name)  VALUES (?, ?)"

    #             for data in search:

    #                 id_Product = data[0]
    #                 list_images = data[1]["product_images"]
    #                 files_name = [img for img in list_images if img != None]
                    
    #                 for name in files_name:
    #                     conn_lite.execute(query, (id_Product, name,))
    #                     path = os.path.join(path_s3, name)
    #                     path = path.replace("\\", "/")
    #                     file.write(path+"\n")
            
    #         conn_lite.commit()
    #     conn_lite.close()

    #     # descargamos las imagenes del s3
    #     new_file_name = download_images(filename_images, directory_path["img"])

    # if new_file_name != False:

    #     similarity = fd(new_file_name)
        
    #     # cambiamos el la direccion de las imagenes para que solo sea el nombre del archivo
    #     for col_name in ["filename_from", "filename_to"]: 
    #         similarity[col_name] = similarity[col_name].apply(lambda x : os.path.basename(x))
        
    #     with sqlite3.connect(db_name) as conn_lite:
            
    #         #3 validamos que la data que se va ha subir no exista
    #         query = "SELECT id FROM {table_name} WHERE file_name = '{file_name}'"
    #         query2 = f"SELECT origin_id, alternative_id FROM {request_name}.matchings WHERE origin_id = %s AND alternative_id = %s"
    #         query3 = f'INSERT INTO public."ProductsRequest2" (id, "idRequest", "originProducts", distance, "alternativeProducts") VALUES(%s,%s,%s,%s,%s);'
            
    #         for _, row in similarity.iterrows():

    #             filename_from = row["filename_from"]
    #             origin_id = conn_lite.execute(query.format(table_name="origin", file_name=filename_from)).fetchone()

    #             if origin_id:

    #                 filename_to = row["filename_to"]
    #                 alternative_id = conn_lite.execute(query.format(table_name="alternative", file_name=filename_to)).fetchone()
                    
    #                 if alternative_id:
                        
    #                     params = (origin_id[0], alternative_id[0],)
    #                     connl.execute(query2, params)
    #                     result = connl.result.fetchone()

    #                     if result == None:

    #                         query4 = f"SELECT ori.products, alt.products FROM {request_name}.origin ori JOIN {request_name}.alternative alt ON ori.id = %s AND alt.id = %s"
    #                         connl.execute(query4, (origin_id, alternative_id))
    #                         product_origin, alternative_product = connl.result.fetchone()

    #                         product_images_origin = product_origin["product_images"]
    #                         product_origin["product_images"] = {
    #                             "s3_path": s3_path_img_origin,
    #                             "matched_image": row["filename_from"],
    #                             "files_name": product_images_origin
    #                         }

    #                         product_images_alternative = product_origin["product_images"]
    #                         alternative_product["product_images"] = {
    #                             "s3_path": s3_path_img_alternative,
    #                             "matched_image": row["filename_to"],
    #                             "files_name": product_images_alternative
    #                         }

    #                         id_match = str(uuid4)
    #                         product_origin = json.dumps(product_origin)
    #                         alternative_product = json.dumps(alternative_product)
    #                         similarity = row["similarity"]

    #                         params = (id_match, request_id, product_origin, similarity, alternative_product)
    #                         connp.execute(query3, params= params, commit= True)
    #                     else:
    #                         continue
    # else:
    #     print("No hay archivos para poder analizar")
    #     shutil.rmtree(directory_path["root_address"])

    #     query5 = f"UPDATE {request_name}.inputs SET status=false WHERE id= %s"
    #     connl.execute(query5, (input_id, ))
    
    # conn_lite.close()
    # connl.close()
    # connl.close()
    # shutil.rmtree(directory_path["root_address"])
    
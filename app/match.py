# Third-party imports
import os
import json
import shutil
import sqlite3
import fastdup
import pandas as pd
from uuid import uuid4
from fastapi import HTTPException

# Local imports
from .tools.functions import download_images
from .tools.db import DatabaseConnection
from .tools.aws import get_path_s3
from .search import search_db
from .tools.settings import DATABASES, TRASH
from .tools.classes import Input

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
        s3_path_img_alternative: str = None,
        ) -> dict:
    
    connl = DatabaseConnection(**DATABASES)
    connp = DatabaseConnection(**DATABASES)

    connl.connect()
    connp.connect()
    
    request_name = get_request_name(request_id)

    input_request = Input(request_id, input)
    
    if input_request.status == 2: # matching
        
        msm = {
            "Status_input":input_request.status_dict[input_request.status],
            "input_id": input_request.id,
            "input": input_request.request
        }

        raise HTTPException(status_code=409, detail=msm)

    elif input_request.status == 6: # ignore

        msm = {
            "Status_input":input_request.status_dict[input_request.status],
            "input_id": input_request.id,
            "input": input_request.request
        }

        return msm

    search_origin = search_db(
        schema_name= request_name, 
        columns= ["id","products"], 
        parameter= input["origin"], 
        table_name= "origin", 
        conn_params=DATABASES)
    
    search_alternative = search_db(
        schema_name= request_name, 
        columns=["id","products"], 
        parameter= input["alternative"], 
        table_name= "alternative", 
        conn_params=DATABASES)
    
    if search_origin == False or search_alternative == False:

        input_request.status = 4 # couldn't find data from DB

        msm = {
            "msm":input_request.status_dict[input_request.status],
            "input_id": input_request.id,
            "input": input_request.request
        }

        input_request.update()

        raise HTTPException(status_code=409, detail=msm)
    
    directory_path = {
            "root_address": os.path.join(TRASH, str(uuid4())),
        }

    directory_path["img"] = os.path.join(directory_path["root_address"], "img")
    directory_path["db"] = os.path.join(directory_path["root_address"], "db")
    directory_path["fastdup"] = os.path.join(directory_path["root_address"], "fastdup")

    # creamos la carpeta donde vamos a guardar los resultados de fastdup y las imagenes
    db_name =  os.path.join(directory_path["db"] , (request_name + ".sqlite"))
    filename_images = os.path.join(directory_path["fastdup"], "path_images.txt") 

    for path in directory_path.values():
        os.mkdir(path)

    # fin de la fase análisis del input e inicio del proceso de match
    input_request.status = 2 # matching
    input_request.update()
    
    with open(filename_images, "w", encoding="utf-8") as file:
        # creamos una db local para guardar la relacion entre la imagen y el id
        with sqlite3.connect(db_name) as conn_lite:
            
            for name in ["origin", "alternative"]:
                conn_lite.execute(f"CREATE TABLE IF NOT EXISTS {name}(id INTEGER, file_name VARCHAR(1024), s3_path VARCHAR(1024))")

            conn_lite.execute(f"CREATE TABLE IF NOT EXISTS matches (id INGEER, id_origin INTEGER, id_alternative INTEGER, distance FLOAT,\
                                FOREIGN KEY (id_origin) REFERENCES origin(id), FOREIGN KEY (id_alternative) REFERENCES alternative(id))")
            conn_lite.commit()

            for search, table_name, path_s3 in zip([search_origin, search_alternative], ["origin", "alternative"], [s3_path_img_origin, s3_path_img_alternative]):

                cont = 0

                query = f"INSERT INTO {table_name}(id, file_name, s3_path)  VALUES (?, ?, ?)"

                for data in search:

                    id_Product, list_images = data
                    files_name = [img for img in list_images["product_images"] if img != None]
                    # tomamos una muestra para poder buscar su direccion en el s3
                    correct_path_s3 = get_path_s3(files_name[0], path_s3)
                    
                    if correct_path_s3:
                        
                        cont += 1

                        for name in files_name:
                            conn_lite.execute(query, (id_Product, name, correct_path_s3))
                            path = os.path.join(correct_path_s3, name).replace("\\", "/")
                            file.write(path+"\n")

                else:
                    if cont == 0:
                        
                        input_request.status = 5 # couldn't find images to dowload from s3

                        msm = {
                            "Status_input":input_request.status_dict[input_request.status],
                            "input_id": input_request.id,
                            "input": input_request.request
                        }

                        conn_lite.close()
                        connl.close()
                        connp.close()
                        shutil.rmtree(directory_path["root_address"])
                        input_request.update()
        
                        raise HTTPException(status_code=409, detail=msm)
            
            conn_lite.commit()
        conn_lite.close()

        # descargamos las imagenes del s3
        new_file_name = download_images(filename_images, directory_path["img"])

    if new_file_name != False:

        similarity = fd(new_file_name, False)
        
        # cambiamos el la direccion de las imagenes para que solo sea el nombre del archivo
        for col_name in ["filename_from", "filename_to"]: 
            similarity[col_name] = similarity[col_name].apply(lambda x : os.path.basename(x))
        
        with sqlite3.connect(db_name) as conn_lite:
            
            #3 validamos que la data que se va ha subir no exista
            query = 'SELECT ori.id, alt.id, ori.s3_path, alt.s3_path from origin as ori JOIN alternative as alt ON ori.file_name = ? AND alt.file_name = ?'
            query2 = 'SELECT id, distance from matches WHERE id_origin = ? AND id_alternative = ?'
            query3 = 'INSERT INTO public."ProductsRequest" (id, "idRequest", "originProducts", distance, "alternativeProducts") VALUES(%s,%s,%s,%s,%s);'
            query5 = 'INSERT INTO matches (id, id_origin, id_alternative, distance) VALUES (?, ?, ?, ?)'
            query6 = 'UPDATE matches SET distance = ? WHERE id = ?'
            query7 = 'UPDATE public."ProductsRequest" SET  "originProducts" = %s, distance = %s, "alternativeProducts" = %s WHERE id = %s'
            
            cont = 0

            for _, row in similarity.iterrows():

                filename_from = row["filename_from"]
                filename_to = row["filename_to"]
                search_data = conn_lite.execute(query, (filename_from, filename_to)).fetchone()

                if search_data != None:

                    origin_id, alternative_id, origin_path_s3, alternative_path_s3= search_data

                    query4 = f"SELECT ori.products, alt.products FROM {request_name}.origin ori JOIN {request_name}.alternative alt ON ori.id = %s AND alt.id = %s"
                    
                    product_origin, alternative_product = connl.execute(query4, (origin_id, alternative_id)).fetchone()

                    product_images_origin = product_origin["product_images"]
                    product_origin["product_images"] = {
                        "input_id": input_request.id,
                        "s3_path": origin_path_s3,
                        "matched_image": row["filename_from"],
                        "files_name": product_images_origin
                    }

                    product_images_alternative = alternative_product["product_images"]
                    alternative_product["product_images"] = {
                        "input_id": input_request.id,
                        "s3_path": alternative_path_s3,
                        "matched_image": row["filename_to"],
                        "files_name": product_images_alternative
                    }

                    # consultamos si el match ya existe 
                    match_exist = conn_lite.execute(query2, (origin_id, alternative_id)).fetchone()

                    product_origin = json.dumps(product_origin)
                    alternative_product = json.dumps(alternative_product)
                    similarity = row["distance"]

                    if match_exist == None:

                        id_match = str(uuid4())
                        params = (id_match, request_id, product_origin, similarity, alternative_product)
                        connp.execute(query3, params= params)

                        conn_lite.execute(query5, (id_match, origin_id, alternative_id, similarity))
                        conn_lite.commit()

                        cont += 1

                    else:
                        id_match, distance = match_exist

                        if similarity <= distance:

                            conn_lite.execute(query6, (similarity, id_match))
                            conn_lite.commit()

                            connp.execute(query7, (product_origin, similarity, alternative_product, id_match))

                else:
                    continue
            else:
                print(f"Se ha terminado de matchar los productos, en total fue un(os) {cont} matchados.")

                input_request.status = 3
                input_request.update()
    else:

        input_request.status = 5 # couldn't find images to dowload from s3

        msm = {
            "Status_input":input_request.status_dict[input_request.status],
            "input_id": input_request.id,
            "input": input_request.request
        }

        input_request.update()
        conn_lite.close()
        connl.close()
        connp.close()
        shutil.rmtree(directory_path["root_address"])
        raise HTTPException(status_code=409, detail=msm)
    
    conn_lite.close()
    connl.close()
    connp.close()
    shutil.rmtree(directory_path["root_address"])
    
    msm = {
        "msm":input_request.status_dict[input_request.status],
        "number_matches_found": cont,
        "input": input_request.request
    }

    return msm
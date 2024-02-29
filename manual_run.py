from app.tools.s3 import search_correct_s3_path
from app.tools.db import DatabaseConnection
from app.tools.constans import paramsl
from pprint import pprint as pp
from app.main import match_img
import shutil
import json
import os

connl = DatabaseConnection(connect = True, **paramsl)
conn_origin = DatabaseConnection(connect = True, **paramsl)
conn_alternative = DatabaseConnection(connect = True, **paramsl)

def extrac_one_fileName_from_s3(schema_name: str,  table_name: str, brand_name: str) -> str:

    conn = DatabaseConnection(connect=True, **paramsl)
    query = f"SELECT products FROM {schema_name}.{table_name} where products ->> 'brand' = %s limit 1"
    conn.execute(query, (brand_name, ))

    result = conn.result.fetchone()
    exist = result != None

    if exist:
        product_image = [file_name for file_name in result[0]["product_images"] if file_name != None][0]
    else:
        product_image = None
    
    return product_image

PATH = "trash/dicts/"
files_name = os.listdir(PATH)

path_images_origin = ["ajio-myntra/origin/20240131/" ,"ajio-myntra/origin/20240213/"]
path_images_alternative = ["ajio-myntra/alternative/20240131/dev/", "ajio-myntra/alternative/20240202/dev/", "ajio-myntra/alternative/20240201/dev/"]
path_file_inputs = ["trash/dicts/brands_dictionary_kids.json","trash/dicts/brands_dictionary_home.json","trash/dicts/brands_dictionary_men.json","trash/dicts/brands_dictionary_women.json"]


for input_file in path_file_inputs:

    with open(input_file, "r", encoding="utf8") as file:
        brands = json.load(file)

        for brand in brands:

            # en caso de que en la ejecusion anterior de error, borramos las carpetas para estar seguros
            for name in ["db", "fastdup", "img"]:
                shutil.rmtree(f"trash/{name}", ignore_errors=True)
                os.mkdir(f"trash/{name}")

            if "'" in  brand["ajio_brand"] or "'" in brand["myntra_brand"]: continue

            input_origin = {"products":{"brand": brand["ajio_brand"]}}
            input_alternative = {"products":{"brand": brand["myntra_brand"]}}

            # extraemos un file_name de la busqueda para tomarlo como muestra 
            file_name_origin = extrac_one_fileName_from_s3("request_e07aaf8f_a587_42af_a0b3_0abd42a7ffc5", "origin", brand["ajio_brand"])
            file_name_alternative = extrac_one_fileName_from_s3("request_e07aaf8f_a587_42af_a0b3_0abd42a7ffc5", "origin", brand["ajio_myntra"])

            if file_name_origin == None or file_name_alternative == None:
                print(f"la marca {brand['ajio_brand']} no existe en la base de datos")
                continue

            s3_path_img_origin = search_correct_s3_path(path_images_origin, file_name_origin)
            s3_path_img_alternative = search_correct_s3_path(path_images_alternative, file_name_alternative)

            if s3_path_img_origin == None or s3_path_img_alternative == None:
                print(f"No se encuentra la dirección exacta en el s3 donde se encuentran los archivos.")
                continue


            request = {
                "request_id": "e07aaf8f-a587-42af-a0b3-0abd42a7ffc5",
                "input": {
                    "origin":input_origin,
                    "alternative":input_alternative,
                },
                "s3_path_img_origin": s3_path_img_origin,
                "s3_path_img_alternative": s3_path_img_alternative,
                
            }

            query = f"SELECT input, status FROM {'request_'+request['request_id'].replace('-', '_')}.inputs WHERE input = %s"
            params = json.dumps(request["input"])
            connl.execute(query, (params,))
            result = connl.result.fetchone()

            if result == None or result[1] == False:

                try:
                    print(f"Empzamos nuevo matching:\nrequest:\n{request}")
                    match_img(**request, connl=connl, conn_origin=conn_origin, conn_alternative=conn_alternative)
                    print("El match terminó. Revisa el resultado en la Base de datos")
                except Exception as e:
                    print(f"Se produjo un error: {type(e).__name__} - {str(e)}")
                    for name in ["db", "fastdup", "img"]:
                        shutil.rmtree(f"trash/{name}", ignore_errors=True)
                        os.mkdir(f"trash/{name}")

            else:
                print("el input ya esta registrado:")
                pp(request["input"])
        
        else:
            print("Ya se termino de matchar con todos los inputs ingresados")
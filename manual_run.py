from app.tools.db import DatabaseConnection
from app.tools.constans import paramsl
from pprint import pprint as pp
from app.main import match_img
import json
import os

connl = DatabaseConnection(**paramsl)
connl.connect()


PATH = "trash/dicts/"
files_name = os.listdir(PATH)

path_images_origin_s3 = ["ajio-myntra/origin/20240131/" ,"ajio-myntra/origin/20240213/"]
path_images_alternative_s3 = ["ajio-myntra/alternative/20240131/dev/", "ajio-myntra/alternative/20240202/dev/", "ajio-myntra/alternative/20240201/dev/"]
path_file_inputs = ["trash/dicts/brands_dictionary_kids.json","trash/dicts/brands_dictionary_home.json","trash/dicts/brands_dictionary_men.json","trash/dicts/brands_dictionary_women.json"]

request_id = "e07aaf8f-a587-42af-a0b3-0abd42a7ffc5"
request_name = 'request_'+request_id.replace('-', '_')

for input_file in path_file_inputs:

    with open(input_file, "r", encoding="utf8") as file:
        brands = json.load(file)

        for brand in brands:

            inputs = {
                "origin" : {"products":{"brand": brand["ajio_brand"]}},
                "alternative" : {"products":{"brand": brand["myntra_brand"]}}
            }
                
            query = f"SELECT input, status FROM {request_name}.inputs WHERE input = %s"
            params = (json.dumps(inputs), )
            
            result = connl.execute(query, params).fetchone()

            input_, status = result if result != None else None, None

            if input_ == None or status != False:

                request = {
                    "request_id": request_id,
                    "input": inputs,
                    "s3_path_img_origin": path_images_origin_s3,
                    "s3_path_img_alternative": path_images_alternative_s3,
                }

                try:

                    print(f"Empzamos nuevo matching.\nrequest:")
                    pp(request)
                    match_img(**request)
                    print("El match terminó. Revisa el resultado en la Base de datos")

                except Exception as e:

                    pp(f"Se produjo un error: {type(e).__name__} - {str(e)}")

            else:
                print("el input ya esta registrado:")
                pp(inputs)
        
else:
    print("Ya se termino de matchar con todos los inputs ingresados")
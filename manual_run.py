from pprint import pprint as pp
from app.main import match_img
from app.tools.db import DatabaseConnection
from app.tools.constans import paramsl
import json
import shutil
import os

connl = DatabaseConnection(**paramsl)
conn_origin = DatabaseConnection(**paramsl)
conn_alternative = DatabaseConnection(**paramsl)

connl.connect()
conn_origin.connect()
conn_alternative.connect()

PATH = "trash/dicts/"
files_name = os.listdir(PATH)
path_images = ["ajio-myntra/alternative/20240131/dev", "ajio-myntra/alternative/20240202/dev", "ajio-myntra/alternative/20240131/dev"]

path_images_origin = "ajio-myntra/origin/20240131/"

routes = {
    "trash/dicts/brands_dictionary_kids.json": "ajio-myntra/alternative/20240131/dev/",
    "trash/dicts/brands_dictionary_home.json": "ajio-myntra/alternative/20240202/dev/",
    "trash/dicts/brands_dictionary_men.json": "ajio-myntra/alternative/20240131/dev/",
    "trash/dicts/brands_dictionary_women.json": "ajio-myntra/alternative/20240201/dev/"
}

for local_path, s3_path in routes.items():
    with open(local_path, "r", encoding="utf8") as file:
        brands = json.load(file)

        for brand in brands:

            connl.connect()
            conn_origin.connect()
            conn_alternative.connect()

            if "'" in  brand["ajio_brand"] or "'" in brand["myntra_brand"]: continue

            input_origin = {"products":{"brand": brand["ajio_brand"]}}
            input_alternative = {"products":{"brand": brand["myntra_brand"]}}

            request = {
                "request_id": "e07aaf8f-a587-42af-a0b3-0abd42a7ffc5",
                "input": {
                    "origin":input_origin,
                    "alternative":input_alternative,
                },
                "s3_path_img_origin": path_images_origin,
                "s3_path_img_alternative": s3_path,
                
            }

            query = f"SELECT input FROM {'request_'+request['request_id'].replace('-', '_')}.inputs WHERE input = %s"
            params = json.dumps(request["input"])
            connl.execute(query, (params,))
            result = connl.result.fetchone()

            if result == None:

                try:
                    print("Empzamos nuevo matching:")
                    print("request:")
                    pp(request)
                    match_img(**request, connl=connl, conn_origin=conn_origin, conn_alternative=conn_alternative)
                    print(" el match termino revisa el resulatdo en la Base de datos")
                    for name in ["db", "fastdup", "img"]:
                        shutil.rmtree(f"trash/{name}")
                        os.mkdir(f"trash/{name}")
                except TypeError as e:
                    print(f"error con el input:\n{request['input']}")
                    for name in ["db", "fastdup", "img"]:
                        shutil.rmtree(f"trash/{name}")
                        os.mkdir(f"trash/{name}")
                    print(e)

            else:
                print("el input ya esta registrado:")
                pp(request["input"])
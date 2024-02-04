from pprint import pprint as pp
from app.main import match_img
from app.tools.db import DatabaseConnection
from app.tools.constans import paramsl
import json
import shutil
import os

connl = DatabaseConnection(**paramsl)
connl.connect()

PATH = "trash/dicts/"
files_name = os.listdir(PATH)
path_images = ["ajio-myntra/alternative/20240131/dev", "ajio-myntra/alternative/20240202/dev", "ajio-myntra/alternative/20240131/dev"]

path_images_origin = "ajio-myntra/origin/20240131/"

routes = {
    r"trash\dicts\brands_dictionary_kids.json": "ajio-myntra/alternative/20240131/dev/",
    r"trash\dicts\brands_dictionary_home.json": "ajio-myntra/alternative/20240202/dev/",
    r"trash\dicts\brands_dictionary_men.json": "ajio-myntra/alternative/20240131/dev/"
}

for local_path, s3_path in routes.items():
    with open(local_path, "r", encoding="utf8") as file:
        brands = json.load(file)

        for brand in brands:

            input_origin = {"brand": brand["ajio_brand"]}
            input_alternative = {"brand": brand["myntra_brand"]}

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
                    match_img(**request)
                    print(" el match termino revisa el resulatdo en la Base de datos")
                except TypeError as e:
                    for name in ["db", "fastdup", "img"]:
                        shutil.rmtree(f"trash/{name}")
                        os.mkdir(f"trash/{name}")
                    print(e)

            else:
                print("el input ya esta registrado:")
                pp(request["input"])
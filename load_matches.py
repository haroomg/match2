from app.tools.db import  DatabaseConnection
from app.tools.constans import paramsl, paramsp
from app.tools.s3 import validate_path
from pprint import pprint as pp
import ijson, json, os
import uuid

path_input = "trash/dicts/old_matches.json"
conn = DatabaseConnection(**paramsl)
conn1 = DatabaseConnection(**paramsl)
conn2 = DatabaseConnection(**paramsp)

conn.connect()
conn1.connect()
conn2.connect()

query = "SELECT origin_id, alternative_id, similarity FROM request_e07aaf8f_a587_42af_a0b3_0abd42a7ffc5.matchings where id > 196530;"
query1 = "SELECT products FROM request_e07aaf8f_a587_42af_a0b3_0abd42a7ffc5.{table} WHERE id = %s;"
query2 = 'INSERT INTO public."ProductsRequest" (id, "idRequest", "originProducts", distance, "alternativeProducts") VALUES(%s,%s,%s,%s,%s);'

conn.execute(query)
matches = conn.result
request_id = "e07aaf8f-a587-42af-a0b3-0abd42a7ffc5"

s3_path_origin = ["ajio-myntra/origin/20240131/", "ajio-myntra/origin/20240213/"]
s3_path_alternative = ["ajio-myntra/alternative/20240131/dev/", "ajio-myntra/alternative/20240202/dev/", "ajio-myntra/alternative/20240131/dev/", "ajio-myntra/alternative/20240201/dev/"]

def get_path(files: str, paths: str) -> str:
    for file in files:
        if file != None:
            path_files = [validate_path(os.path.join(path,file)) for path in paths]
            if any(path_files):
                return paths[path_files.index(True)]
    else:
        return None
    

cont = 0

for match in matches:

    similarity = match[2]
    id_uid = uid = str(uuid.uuid4())

    conn1.execute(query1.format(table="origin"), (match[0],))
    product_origin = conn1.result.fetchone()[0]
    product_images_origin = product_origin["product_images"]

    path_s3_origin = get_path(product_images_origin, s3_path_origin)
    
    product_origin["product_images"] = {
        "s3_path": path_s3_origin,
        "files_name": product_images_origin
    }
    
    product_origin = json.dumps(product_origin)


    conn1.execute(query1.format(table="alternative"), (match[1],))
    product_alternative = conn1.result.fetchone()[0]
    product_images_alternative = product_alternative["product_images"]

    path_s3_alternative = get_path(product_images_alternative, s3_path_alternative[1:])
    
    product_alternative["product_images"] = {
        "s3_path": path_s3_alternative,
        "files_name": product_images_alternative
    }
    
    product_alternative = json.dumps(product_alternative)

    params = (id_uid, request_id, product_origin, similarity, product_alternative)
    conn2.execute(query2, params)

    cont += 10000

    if cont == 10000:
        cont = 0
        conn2.commit()

conn2.commit()
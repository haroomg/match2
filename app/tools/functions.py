from .db import DatabaseConnection
from .constans import BUCKET
from hashlib import sha256
from .s3 import client
import concurrent
import datetime
import imageio
import ijson
import json
import os

def create_load_data(
    shema_name: str = "public",
    table_name: str = None, 
    path_file: str = None,
    conn = None
    ) -> None:
    
    query = f"CREATE TABLE IF NOT EXISTS {shema_name}.{table_name}(id SERIAL PRIMARY KEY, products JSONB)"
    conn.execute(query)
    conn.commit()

    with open(path_file, "r", encoding="utf8") as file:
        json_file = ijson.items(file, "item")

        query = f"INSERT INTO {shema_name}.{table_name}(products) VALUES(%s)"
        cont = 0

        for obj in json_file:
            conn.execute(query, (json.dumps(obj),))
            cont += 1
    
        print(f"La tabla {table_name} fue creada en el shema {shema_name}.")
        print(f"Un total de {cont} filas fueron ingresados en la tabla {table_name}.")
        conn.commit()
    
    return


def search_db(
    schema_name: str = "public",
    table_name: str = None, 
    columns: list = "*",
    parameter: dict = None,
    conn_params: dict = None 
    ) -> None:

    conn = DatabaseConnection(**conn_params)
    conn.connect()

    if "operator" in parameter:
        operator = parameter["operator"].upper()
        del  parameter["operator"]
    else:
        operator = "AND"
    
    if columns != "*":
        col = ", ".join(columns)
    else:
        col = columns
    
    query = f"SELECT {col} FROM {schema_name}.{table_name} WHERE "

    for key, value in parameter.items():

        if isinstance(value, str):
            value = value.replace("'","''")
            query += f"({key} = '{value}') {operator} "

        if isinstance(value, int):
            query += f"({key} = {value}) {operator} "

        if isinstance(value, float):
            query += f"({key} = {value}) {operator} "

        if isinstance(value, bool):
            if value:
                value = "true"
            else:
                value = "false"
            query += f"({key} = {value}) {operator} "

        if isinstance(value, list):
            values = []
            for val in value:
                if isinstance(val, str):
                    val = val.replace("'", "''")
                    values.append(f"'{val}'")
                if isinstance(val, (int, float)):
                    values.append(val)
            values = ", ".join(values)
            query += f"({key} IN ({values})) {operator} "

        if isinstance(value, dict):
            for name, vl in value.items():
                if isinstance(vl, str):
                    vl = vl.replace("'", "''")
                    query += f"({key}->> '{name}' = '{vl}') {operator} "
                if isinstance(vl, (int, float)):
                    query += f"({key}->> '{name}' = {vl}) {operator} "
                if isinstance(vl, bool):
                    if vl:
                        vl = "true"
                    else:
                        vl = "false"
                    query += f"({key}->> '{name}' = {vl}) {operator} "
        
    query = query[:-(len(operator)+2)]
    result = conn.execute(query).fetchall()

    if len(result):
        return result
    else:
        return False


def add_metadata(
        img_path: str = None, 
        metadata: dict = None
        ) -> str:
    
    if not os.path.exists(img_path):
        raise ValueError(f"La Dirección proporcionada no existe o esta mal escrita:\n{img_path}")
    
    if not metadata:
        # Si el usario no define una metadata, nosotros agregamos
        metadata = {
            "msm": "Esta imagen no contenia Metadata",
            "repair_day": datetime.date.today()
        }
    elif not isinstance(metadata, dict):
        raise ValueError(f"El atributo de Metadata debe ser de tipo dict, no de tipo {type(metadata).__name__}")
    
    try:
        # file_name = os.path.basename(img_path)
        
        image = imageio.imread(img_path)
        # re-escribimos la imagen con la metadata agregada
        imageio.imwrite(img_path, image, **metadata)
        # print(f"Se acaba de agregar la metadata en el archivo {file_name}")
    
    #3
    except TypeError as e:
        print(e)
        
    finally:
        return img_path


def download_images(path_images_s3: str = None, destination_folder: str = None) -> str:
    if path_images_s3 is None or destination_folder is None:
        raise ValueError("Los argumentos 'path_images_s3' y 'destination_folder' no pueden ser None.")

    downloaded_images = []

    def download_image(imagen: str = None) -> None:
        imagen = imagen.replace("\n", "")
        file_name = os.path.basename(imagen)
        path = os.path.join(destination_folder, file_name).replace("\\", "/").replace("\n", "")

        try:
            if not os.path.exists(path):  # Verificar si la imagen ya existe en el destino
                client.download_file(BUCKET, imagen, path)
            
            downloaded_images.append(path + "\n")
            
            # modificamos la metadata de la imagen y en caso de que no tenga se la agregamos
            try:
                add_metadata(path)
            except Exception as e:
                print(f"La metadata del archivo no pudo ser modificada:\nFile:{path}\nError:{str(e)}")

        except Exception as e:
            print(f"Error al descargar la imagen {imagen}: {str(e)}")

    with open(path_images_s3, 'r') as images_s3:

        with concurrent.futures.ThreadPoolExecutor() as executor:
            file_copy = os.path.join(os.path.dirname(path_images_s3), "local_path_files.txt").replace("\\", "/")

            with open(file_copy, "w") as file:
                futures = []

                # Inicia la descarga de las imágenes en paralelo
                for image_s3 in images_s3:
                    futures.append(executor.submit(download_image, image_s3))
                
                # Espera a que todas las descargas se completen
                concurrent.futures.wait(futures)

                if len(downloaded_images) > 0:
                    for downloaded_image in downloaded_images:
                        file.write(downloaded_image)
                    
                    print(f"Un total de {len(downloaded_images)} imagen(es) ha(n) sido descargada(s).")
                    return file_copy

                else:
                    print("No se pudo descargar ninguna imagen. Verifica las direcciones en el S3.")
                    return False


def generate_hash(value1: str = None ,value2: str = None) -> sha256:

    concat_values = value1 + value2

    hash_object = sha256(concat_values.encode())
    hash_result = hash_object.hexdigest()

    return hash_result
from fastapi import HTTPException
from pprint import pprint as pp
from .constans import BUCKET
from .s3 import client
import pandas as pd
import concurrent
import itertools
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


def search(
    schema_name: str = "public",
    table_name: str = None, 
    comlumns: list = "*",
    parameter: dict = None, 
    conn = None
    ) -> None:

    if "operator" in parameter:
        operator = parameter["operator"].upper()
        del  parameter["operator"]
    else:
        operator = "AND"
    
    if comlumns != "*":
        col = ", ".join(comlumns)
    else:
        col = comlumns
    
    query = f"SELECT {col} FROM {schema_name}.{table_name} WHERE "

    for key, value in parameter.items():

        if isinstance(value, str):
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
                    values.append(f"'{val}'")
                if isinstance(val, (int, float)):
                    values.append(val)
            values = ", ".join(values)
            query += f"({key} IN ({values})) {operator} "

        if isinstance(value, dict):
            for name, vl in value.items():
                if isinstance(vl, str):
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
    conn.execute(query)

    test, result = itertools.tee(conn.result)
    

    try:
        # si itera es que contiene informacion
        next(test)
        del test
        return result
    except:
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


def download_images(
        local_path_images: str = None, 
        destination_folder: str = None
    ) -> str:

    def download_image(imagen: str = None) -> None:

        file_name = os.path.basename(imagen)
        path = os.path.join(destination_folder, file_name).replace("\\", "/")

        try:
            client.download_file(BUCKET, imagen, path)
            # modificamos la metadata de la imagen y en caso de que no tenga se la agregamos
            try:
                add_metadata(path)
            except:
                print(f"La metadata del archivo no pudo ser modificada:\nFile:{path}")

        except Exception as e:
            print(f"Error al descargar la imagen {imagen}: {str(e)}")


    with open(local_path_images, 'r') as images:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            file_copy = os.path.join(os.path.dirname(local_path_images), "local_path_files.txt").replace("\\", "/")

            with open(file_copy, "w") as file:

                futures = []

                # Inicia la descarga de las imágenes en paralelo
                for image in images:

                    futures.append(executor.submit(download_image, image.replace("\n", "")))
                    file_name = os.path.basename(image)
                    path_img = os.path.join(destination_folder, file_name)
                    path_img = path_img.replace("\\", "/")

                    #3
                    file.write(path_img)
            
            # Espera a que todas las descargas se completen
            concurrent.futures.wait(futures)

    print("Todos los archivos acaban de ser descargados.")
    print(f"El archivo {local_path_images} acaba de ser midificado.")

    return file_copy
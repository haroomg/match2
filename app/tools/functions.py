from hashlib import sha256
from .aws import S3
import concurrent
import datetime
import imageio
import os


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

    s3 = S3()

    downloaded_images = []

    def download_image(imagen: str = None) -> None:
        
        imagen = imagen.replace("\n", "")
        file_name = os.path.basename(imagen)
        path = os.path.join(destination_folder, file_name).replace("\\", "/").replace("\n", "")

        try:
            if not os.path.exists(path):  # Verificar si la imagen ya existe en el destino

                s3.download_file(imagen, destination_folder)
            
            downloaded_images.append(path + "\n")
            
            # modificamos la metadata de la imagen y en caso de que no tenga se la agregamos
            try:
                add_metadata(path)
            except Exception as e:
                print(f"La metadata del archivo no pudo ser modificada:\nFile:{path}\nError:{str(e)}")

        except Exception as e:
            print(f"Error al descargar la imagen {imagen}:\n{str(e)}\n")

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
# Third-party imports
from shutil import rmtree
from typing import Union
from uuid import uuid4
import json, ijson
import os

# Local imports
from .db import DatabaseConnection
from .settings import DATABASES, TRASH
from .aws import S3

class Input:

    def __init__(self, id_request: str = None, request: dict = None) -> None:
        
        self.id: str = None
        self.id_request: str = id_request
        self.request: dict = request
        self.status: int = None
        self.exist: bool = None
        self.status_dict = None

        self.search()

        if self.exist == False:
            self.create()


    def search(self) -> None:

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        input_request = json.dumps(self.request)

        query = 'SELECT id, status FROM public."inputsMatch" WHERE "idRequest" = %s AND input = %s'
        result = conn.execute(query, (self.id_request, input_request)).fetchone()

        self.__get_status__()

        if result != None:

            self.id = result[0]
            self.status = result[1]
            self.exist = True
        
        else:

            self.exist = False
        
        conn.close()


    def create(self) -> None:

        if self.exist == False:

            conn = DatabaseConnection(**DATABASES)
            conn.connect()

            input_request = json.dumps(self.request)

            self.id = str(uuid4())
            self.status = 1
            self.exist = True

            query = 'INSERT INTO public."inputsMatch"(id, "idRequest", input, status) VALUES(%s,%s,%s,%s)'

            conn.execute(query, (self.id , self.id_request, input_request, self.status))
            conn.commit()
            conn.close()
        
        else:

            print("El Input ya existe, no es necesario crearlo de nuevo.")


    def update(self) -> None:
        
        if self.exist:
            conn = DatabaseConnection(**DATABASES)
            conn.connect()

            input_request = json.dumps(self.request)

            query = 'UPDATE public."inputsMatch" SET input = %s, status = %s WHERE id = %s'
            conn.execute(query, (input_request, self.status, self.id))
            conn.commit()
        else:
            print("No puedes actualizar un input que no existe.")
    

    def delte(self, for_ever: bool = False) -> None:

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        if not for_ever:

            query = 'UPDATE public."inputsMatch" SET status = %s WHERE id= %s;'

            self.status = 6

            conn.execute(query, (self.status, self.id))
        
        else:

            query = 'DELETE FROM public."inputsMatch" WHERE id = %s'

            conn.execute(query, (self.id, ))
        
        self.exist = False
        
        conn.commit()
        conn.close()
    

    def __get_status__(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT id, status FROM public."statusInput"'
        self.status_dict = {id:status for id, status in conn.execute(query).fetchall()}


#3
class Request:

    def __init__(self, request_id):

        # request 
        self.request_id = request_id
        self.filesPath = None
        self.exists_request = False

        self.status = None
        self.status_dict = None
        self.status_msm = None
        
        # other schema
        self.schema_name = "request_" + self.request_id.replace("-", "_")
        self.exists_schema = False

        self.origin_files = None
        self.alternative_files = None
        self.origin_images = None
        self.alternative_images = None

        self.is_load = False

        self.search()
    

    def __get_status__(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT id, status FROM public."RequestStatus"'
        result = conn.execute(query).fetchall()
        self.status_dict = {id:status for id, status in result}
        conn.close()


    def search(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT "filesPath", "requestStatusId" FROM public."Request" WHERE "id" = %s'
        query2 = 'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)'
        query3 = f'SELECT origin_files, alternative_files, origin_images, alternative_images, is_load FROM {self.schema_name}."filesPath"'

        result = conn.execute(query, (self.request_id,)).fetchone()
        exists_schema = conn.execute(query2, (self.schema_name,)).fetchone()[0]

        self.__get_status__()

        if result != None:

            self.filesPath = result[0]
            self.request_status = result[1]
            self.status_msm = self.status_dict[result[1]]
            self.exists_request = True
            self.exists_schema = exists_schema
        
        if self.exists_schema == True:

            self.origin_files,\
            self.alternative_files,\
            self.origin_images,\
            self.alternative_images,\
            self.is_load= conn.execute(query3).fetchone()

        conn.close()



    def create_schema(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        if self.exists_request:
            if self.exists_schema == False:

                query = f"CREATE SCHEMA {self.schema_name};"
                conn.execute(query)
                conn.commit()
                
                query2 = "CREATE TABLE IF NOT EXISTS {schema}.{name}(id VARCHAR(64), products JSONB)"

                for name in ["origin", "alternative"]:
                    conn.execute(query2.format(schema=self.schema_name, name=name))
                conn.commit()
                
                query3 = f'CREATE TABLE IF NOT EXISTS {self.schema_name}."filesPath"(\
                    origin_files TEXT[],\
                    alternative_files TEXT[],\
                    origin_images TEXT[],\
                    alternative_images TEXT[],\
                    is_load BOOLEAN DEFAULT false\
                        )'
                
                self.origin_files = self.filesPath["origin_files"]
                self.alternative_files = self.filesPath["alternative_files"]

                self.origin_images = self.filesPath["origin_images"]
                self.alternative_images = self.filesPath["alternative_images"]

                query4 = f'INSERT INTO {self.schema_name}."filesPath"\
                    (origin_files, alternative_files, origin_images, alternative_images)\
                    VALUES(%s,%s,%s,%s)'

                conn.execute(query3)
                conn.execute(query4, (self.origin_files, self.alternative_files,\
                                    self.origin_images, self.alternative_files))

                conn.commit()
                conn.close()

                self.exists_schema = True

            else:
                print("El schema ya existe.")
        else:
            print("No se puede crear un schema de un request que no existe.")
    

    def update_filesPath(self):

        if self.exists_schema:

            conn = DatabaseConnection(**DATABASES)
            conn.connect()

            query = f'UPDATE {self.schema_name}."filesPath" SET \
            origin_files= %s, alternative_files= %s, origin_images= %s, alternative_images= %s, is_load= %s;'

            params = (self.origin_files, self.alternative_images, self.origin_images, self.alternative_images, self.is_load)

            conn.execute(query, params)
            conn.commit()
            conn.close()
        else:
            print("el schema no existe.")


    def add_path(self, list_path, new_path):

        if isinstance(new_path, str):

            new_path = [new_path]
        
        for path in new_path:

            if path not in list_path:

                list_path.append(path)
        
        self.update_filesPath()
    

    def delete_path(self, list_path, path_to_delete):
        
        if isinstance(list_path, str):
            list_path = [list_path]

        for path in list_path:

            if path in list_path:
                list_path.remove(path)
                self.update_filesPath()
            else:
                print(f"'{path_to_delete}' no existe.")


    def load_data(self):

        if self.exists_request:
            if self.exists_schema:

                conn = DatabaseConnection(**DATABASES)
                conn.connect()

                s3 = S3()

                dir_name = str(uuid4())
                local_dir = os.path.join(TRASH, dir_name)
                os.makedirs(local_dir)

                if self.is_load == False:

                    path_and_name = {
                        "origin": self.origin_files,
                        "alternative": self.alternative_files
                    }

                    for name, files_list in path_and_name.items():

                        for path_s3 in files_list:

                            # validamos el path_s3
                            if s3.validate_path(path_s3) == False:
                                print(f"La direccion en el s3 no existe o esta mas escriot:\{path_s3}")
                                continue

                            local_file = s3.download_file(path_s3, local_dir)
                            
                            with open(local_file, "r", encoding="utf8") as file:

                                json_file = ijson.items(file, "item")
                                query2 = f"INSERT INTO {self.schema_name}.{name}(id, products) VALUES(%s, %s)"

                                for obj in json_file:

                                    conn.execute(query2, (str(uuid4()), json.dumps(obj)))

                            os.remove(local_file)
                            conn.commit()

                    self.is_load = True

                    self.update_filesPath()
                    conn.close()

                    rmtree(local_dir)

                else:

                    # en caso que de la data ya se subio validamos que se haya agregado una nueva ruta para descargarla y subirla
                    
                    len_origin_diferent = len(self.origin_files) != self.filesPath["origin_files"]
                    len_alternative_diferent = len(self.alternative_files) != self.filesPath["alternative_files"]

                    if len_origin_diferent or len_alternative_diferent:

                        dir_name = str(uuid4())
                        local_dir = os.path.join(TRASH, dir_name)
                        os.makedirs(local_dir)

                        if len_origin_diferent:

                            for origin_path in self.filesPath["origin_files"]:

                                if origin_path not in self.origin_files:

                                    # validamos el path_s3
                                    if s3.validate_path(origin_path) == False:
                                        print(f"La direccion en el s3 no existe o esta mas escriot:\{path_s3}")
                                        continue

                                    local_path_origin = s3.download_file(origin_path, local_dir)
                                    self.origin_files.append(origin_path)

                                    with open(local_path_origin, "r", encoding="utf8") as origin_file:

                                        json_file = ijson.items(origin_file, "item")
                                        query2 = f"INSERT INTO {self.schema_name}.origin(id, products) VALUES(%s, %s)"

                                        for obj in json_file:

                                            conn.execute(query2, (str(uuid4()), json.dumps(obj)))

                                        os.remove(local_path_origin)
                                        self.update_filesPath()
                                        conn.commit()
                        
                        if len_alternative_diferent:

                            for alternative_path in self.filesPath["alternative_files"]:

                                if alternative_path not in self.alternative_files:

                                    # validamos el path_s3
                                    if s3.validate_path(alternative_path) == False:
                                        print(f"La direccion en el s3 no existe o esta mas escriot:\{path_s3}")
                                        continue

                                    local_path_alternative = s3.download_file(alternative_path, local_dir)
                                    self.alternative_files.append(alternative_path)

                                    with open(local_path_alternative, "r", encoding="utf8") as alternative_file:

                                        json_file = ijson.items(alternative_file, "item")
                                        query2 = f"INSERT INTO {self.schema_name}.alternative(id, products) VALUES(%s, %s)"

                                        for obj in json_file:

                                            conn.execute(query2, (str(uuid4()), json.dumps(obj)))

                                        os.remove(local_path_alternative)
                                        self.update_filesPath()
                                        conn.commit()
                                        
                        rmtree(local_dir)
                        conn.close()

                    else:
                        print("Los arhivos ya fueron subidos a la DB.")
            else:
                print("El schema no existe.")
        else:
            print("El request no existe.")
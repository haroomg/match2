# Third-party imports
from typing import Union
from uuid import uuid4
import json, ijson
import os

# Local imports
from .db import DatabaseConnection
from .settings import DATABASES, TRASH
from aws import S3

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
        
        conn.close()
    

    def get_status(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT id, status FROM public."statusInput"'
        self.status_dict = {id:status for id, status in conn.execute(query).fetchall()}


#3
class Request:

    def __init__(self, request_id):

        self.request_id = request_id
        self.filesPath = None
        self.status = None
        self.exists_request = False
        self.schema_name = "request_" + self.request_id.replace("-", "_")
        self.exists_schema = False
        self.status_dict = None

        self.search()
        self.get_status()
    

    def search(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT "filesPath", "requestStatusId" FROM public."Request" WHERE "id" = %s'
        query2 = 'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)'
        result = conn.execute(query, (self.request_id,)).fetchone()
        schema_exists = conn.execute(query2, (self.schema_name,)).fetchone()[0]

        if result != None:

            self.filesPath = result[0]
            self.request_status = result[1]
            self.exists_request = True
            self.schema_exists = schema_exists

        conn.close()


    def get_status(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        query = 'SELECT id, status FROM public."RequestStatus"'
        result = conn.execute(query).fetchall()
        self.status_dict = {id:status for id, status in result}

        conn.close()


    def create_schema(self):

        conn = DatabaseConnection(**DATABASES)
        conn.connect()

        if self.exists_request:
            if self.exists_schema == False:

                query = f"CREATE SCHEMA {self.schema_name};"
                conn.execute(query)
                
                query2 = "CREATE TABLE IF NOT EXISTS {schema}.{name}(id VARCHAR(35), products JSONB)"

                for name in ["origin", "alternative"]:
                    conn.execute(query2.format(schema=self.schema_name, name=name))
                
                conn.close()

                self.schema_exists = True

            else:
                print("El schema ya existe.")
        else:
            print("No se puede crear un schema de un request que no existe.")
    

    def load_data(self):

        if self.exists_request:
            if self.exists_schema:

                conn = DatabaseConnection(**DATABASES)
                conn.connect()

                s3 = S3()

                dir_name = str(uuid4())
                local_dir = os.path.join(TRASH, dir_name)
                os.makedirs(local_dir)
                
                for key_name, name in zip(["origin_files", "alternative_files"], ["origin","alternative"]):

                    for paths_s3 in self.filesPath[key_name]:

                        for path_s3 in paths_s3:
                            
                            local_file = s3.download_file(path_s3, local_dir)
                            
                            with open(local_file, "r", encoding="utf8") as file:

                                json_file = ijson.items(file, "item")
                                query = f"INSERT INTO {self.schema_name}.{name}(id, products) VALUES(%s, %s)"

                                for obj in json_file:

                                    conn.execute(query, (str(uuid4()), json.dumps(obj)))

                            os.remove(local_file)

            else:
                print("El schema no existe.")
        else:
            print("El request no existe.")

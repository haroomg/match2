# Third-party imports
import json
from uuid import uuid4
from typing import Union

# Local imports
from .tools.db import DatabaseConnection
from .tools.constans import *

class Input:

    def __init__(self, id_request: str = None, request: dict = None) -> None:
        
        self.id: str = None
        self.id_request: str = id_request
        self.request: dict = request
        self.status: int = None
        self.exist: bool = None

        self.search()

        if self.exist == False:
            self.create()


    def search(self) -> None:

        conn = DatabaseConnection(**paramsp)
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

            conn = DatabaseConnection(**paramsp)
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
            conn = DatabaseConnection(**paramsp)
            conn.connect()

            input_request = json.dumps(self.request)

            query = 'UPDATE public."inputsMatch" SET input = %s, status = %s WHERE id = %s'
            conn.execute(query, (input_request, self.status, self.id))
        else:
            print("No puedes actualizar un input que no existe.")
    

    def delte(self, for_ever: bool = False) -> None:

        conn = DatabaseConnection(**paramsp)
        conn.connect()

        if not for_ever:

            query = 'UPDATE public."inputsMatch" SET status = %s WHERE id= %s;'

            self.status = 5

            conn.execute(query, (self.status, self.id))
        
        else:

            query = 'DELETE FROM public."inputsMatch" WHERE id = %s'

            conn.execute(query, (self.id, ))
        
        self.exist = False
        
        conn.close()
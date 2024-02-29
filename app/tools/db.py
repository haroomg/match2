import psycopg2

class DatabaseConnection:

    def __init__(self, host, port, database, user, password, connect = False):

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.on = False
        self.off = True

        self.conn = None
        self.cursor = None

        if connect:
            self.connect()

        




    def connect(self):

        if self.off:
            self.conn = psycopg2.connect(
                host=self.host,
                port= self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()

            self.on = True
            self.off = False
        
        else:
            print("La conexión a la base de datos ya esta establecida.")


    def execute(self, query, params=None):
        
        if self.on:
            try:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                self.result = self.cursor
                self.conn.commit()
            except psycopg2.Error as error:
                # Manejar el error o realizar alguna acción según sea necesario
                print("Ocurrió un error:", error)
                self.conn.rollback()
        else:
            print("La conexión fue cerrada. No se puede hacer un execute")


    def commit(self):

        if self.on:
            self.conn.commit()
        else:
            print("La conexión fue cerrada. No se puede hacer commit")


    def rollback(self):

        if self.on:
            self.conn.rollback()
        else:
            print("La conexión fue cerrada. No se puede hacer un rollback")


    def close(self, commit: bool = True):

        if self.off:

            self.on = False
            self.off = True

            self.cursor.close()
            self.conn.close()

            if commit:
                self.commit()
        
        else:
            print("La conexión ya fue cerrada.")
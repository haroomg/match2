from fastapi import HTTPException
import pandas as pd
import ijson
import os

def create_load_data(
    shema_name: str = "public",
    table_name: str = None, 
    path_file: str = None,
    conn = None
    ) -> None:
    
    """
    apartir de la direccion de un json, crea una tabla y sube los datos del json
    Returns:
        _type_: None
    """
    
    if not table_name:
        table_name = os.path.basename(path_file).split(".")[0]
    
    # Función para obtener el tipo de dato de un elemento
    def obtener_tipo_dato(elemento):
        return type(elemento).__name__

    # Aplicar la función a cada elemento de la columna
    df = pd.read_json(path_file)
    df = dtype = df.map(obtener_tipo_dato).drop_duplicates().loc[0].to_dict()
    del df
    
    for name, ty in dtype.copy().items():
        if ty == "int":
            dtype[name] = "BIGINT"
        elif ty == "float":
            dtype[name] = "DOUBLE PRECISION"
        elif ty == "bool":
            dtype[name] = "BOOLEAN"
        elif ty == "str":
            dtype[name] = "TEXT"
        elif ty == "list":
            dtype[name] = "JSONB"
        elif ty == "dict":
            dtype[name] = "JSONB"
        else:
            dtype[name] = "TEXT"
    
    content_table = "id SERIAL PRIMARY KEY, "
    for name, value in dtype.items():
        content_table += f"{name} {value}, "
    content_table = content_table[:-2]
    
    query = f"CREATE TABLE IF NOT EXISTS {shema_name}.{table_name}({content_table})"
    conn.execute(query)
    conn.commit()
    
    with open(path_file, "r", encoding="utf-8") as json_file:
        
        objets_json = ijson.items(json_file, "item")
        
        query = f"INSERT INTO {shema_name}.{table_name}({', '.join(dtype.keys())})" 
        copy = query
        
        for obj in objets_json:
            params = []
            
            for val in obj.values():
                if isinstance(val, (str, int, float)):
                    params.append(val)
                if isinstance(val, (dict, tuple, list)):
                    params.append(str(val).replace("'", "\"").replace("None", "null"))
            
            query += f" VALUES ({', '.join(['%s'] * len(params))})"
            conn.execute(query, tuple(params))
            query = copy
        
        conn.commit()
    
    return


def search(
    request_id: str = None,
    comlumns: list = "*",
    input: dict = None, 
    table_name: str = None, 
    conn = None
    ) -> None:
    
    shema = "request_" + request_id.replace("-", "_")
    
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{shema}'
    AND table_name = '{table}';
    """
    
    
    def process_data(data: list = None) -> None:
        
        data_table = {}
        
        for dt in data:
            
            name = dt[0]
            ty = dt[1]
            
            if ty == "text":
                data_table[name] = str
            elif ty == "integer" or ty == "bigint":
                data_table[name] = int
            elif ty == "double precision":
                data_table[name] = float
            elif ty == "boolean":
                data_table[name] = bool
            elif ty == "jsonb":
                data_table[name] = [dict, list, tuple]
        
        return data_table
    
    
    def is_in(key_name: str = None, columns: list = None) -> tuple:
        
        not_in: list = []
        
        if isinstance(input, dict):
            
            if len(input):
                
                for key in input.keys():
                    
                    if key not in columns:
                        not_in.append(key)
                
                if len(not_in) == 0:
                    return True, None
                
                else:
                    return False, {
                        "msm": f"Las sigientes columnas no se encuentran en la tabla {key_name}",
                        f"not_in_{key_name}": not_in
                    }
            else:
                return False, {
                    "msm": f"El input no puede estar vacio, debe tener al menos un parametro de busqueda"
                }
        else:
            return False, {
                "msm": f"El input debe de ser de tipo dict no de {type(input[key_name]).__name__}."
            }

    
    conn.execute(query.format(shema=shema, table=table_name))
    data_table = process_data(conn.result.fetchall())
    
    # validamos que el nombre de las columnas este bien
    is_ok, msm = is_in(table_name, data_table.keys())
    
    # si algo esta mal retornamos el error
    if not is_ok:
        raise HTTPException(status_code=402, detail=msm)
    
    if "logic" in input:
        logic = input["logic"].upper()
    else:
        logic = "OR"
    
    # si todo sale bien, empezamos a construir el query
    query: str = f"SELECT {comlumns} FROM {shema}.{table_name} WHERE "
    
    for name, value in input.items():
        
        field_type = data_table[name]
        
        if field_type == str:
            if isinstance(value, str):
                query += f"({name} = '{value}') {logic} "
            if isinstance(value, list):
                values = ", ".join([f"'{vl}'" for vl in value])
                query += f"({name} IN ({values})) {logic} "
        
        elif field_type == int or field_type == float:
            if isinstance(value, (int,float)):
                query += f"({name} = {value}) {logic} "
            if isinstance(value, list):
                values = ", ".join([str(vl) for vl in value])
                query += f"({name} IN ({values})) {logic} "
            if isinstance(value, str):
                start, end = [int(vl) for vl in value.split(":")]
                query += f"({name} BETWEEN {start} AND {end}) {logic} "
        
        elif field_type == bool:
            if isinstance(value, bool):
                value_bool = "true" if value else "false"
                query += f"({name} = {value_bool}) {logic} "
    
    query = query[:-4]
    conn.execute(query)
    
    return conn.result
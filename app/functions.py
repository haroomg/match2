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
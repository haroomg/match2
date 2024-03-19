from .tools.db import DatabaseConnection

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
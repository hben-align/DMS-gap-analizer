import boto3
import json
import pyodbc
import psycopg2
from IPython.display import display, clear_output
import sys

def compare_table(table_name: str, sql_server_secret: list = None, postgres_secret: list = None, 
    sql_host = None, sql_user = None, sql_password = None, sql_database = None, 
    pg_host = None, pg_user = None, pg_password = None, pg_database = None,pg_table = None, debug = 0):
    
  

    # using try-finally to close connections
    try:    
        # define sql server secret
        if sql_server_secret is None:
            try:
                sql_server_secret = {k:v for k,v in [('host', sql_host), ('user',sql_user), ('password',sql_password), ('database',sql_database)]}
                display("SQL Server Secret json:", "{}".format({v:sql_server_secret[v] if v != 'password' else  '*********' for v in sql_server_secret}))
            except:
                raise ValueError("\nIf you dont specify secret name for sql server,\nyou must specify hostname, user, password and database for the connection string\n")
        elif len(sql_server_secret) != 2 and isinstance(sql_server_secret, list):
            raise ValueError("\nsql_server_secret must be a list of secret name and region name, for example: ['secret_name', 'region'].\nif you dont want to use secret, specify all sql parameters and leave sql_server_secret empty\n")
        else:
            display("secret value is specified, ignoring all sql_parameters")
        sql_server_connection = get_sql_server_connection(sql_server_secret)

        # define postgres secret
        if postgres_secret is None:
            try:
                postgres_secret = {k:v for k,v in [('host', pg_host), ('user',pg_user), ('password',pg_password), ('database',pg_database)]}
                display("PostgreSQL Secret json:","{}".format({v:postgres_secret[v] if v != 'password' else  '*********' for v in postgres_secret}))
            except:
                raise ValueError("\nIf you dont specify secret name for postgres,\nyou must specify hostname, user, password and database for the connection string\n")
        elif len(postgres_secret) != 2 and isinstance(postgres_secret, list):
            raise ValueError("\npostgres_secret must be a list of secret name and region name, for example: ['secret_name', 'region'].\nif you dont want to use secret, specify all pg parameters and leave postgres_secret empty\n")
        else:
            display("secret value is specified, ignoring all pg_parameters\n")
        pg_connection = get_pg_connection(postgres_secret)

        # get table definition
        columns, table_name = get_table_definition(sql_server_connection, table_name)

        # configuring pg table name
        if pg_table is None:
            pg_table = table_name

        # check table on postgres
        check_if_exists_on_pg(pg_connection,table_name, pg_table, columns)

        if columns['datecreated'] != '':
            gap = calculate_datecreated(sql_server_connection, pg_connection, table_name, pg_table, debug = debug)
            if columns['int identity'] != '':
                gap = identity_for_datecreated(sql_server_connection, table_name, columns['int identity'], gap, debug)
                gap = calculate_identity(sql_server_connection, pg_connection, table_name, columns['int identity'], pg_table, gap, 10000, debug)
            
        elif columns['int identity'] != '':
            gap = calculate_identity(sql_server_connection, pg_connection, table_name, columns['int identity'], pg_table, debug = debug)

        else:
            gap = calculate_total_gap(sql_server_connection, pg_connection, table_name, pg_table)
            raise ValueError('\nThere is no dateCreated column and no Identidy column on {}\nIn this case, manual work is needed.\nTotal gap is:{}'.format(table_name,gap))

        # gap = calculate_gap(sql_server_connection, pg_connection, table_name, column, pg_table, debug = debug)

        clear_output(wait=True) if debug == 0 else None
        if gap['missing_rows'] == [] and gap['gap_on_pg'] == []:
            display('There is no gap between {} and {} in {}'.format(postgres_secret['host'],postgres_secret['host'],table_name))
        else:
            query = building_queries(sql_server_secret['database'],table_name, pg_table, gap)
            print(query)
    finally:
        sql_server_connection.close()
        pg_connection.close()
    return gap
def get_pg_connection(postgres_secret):
    try:
        if isinstance(postgres_secret, dict):
            None
        else:
            secret_id = postgres_secret[0]
            region_name = postgres_secret[1]
            client = boto3.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            response = client.get_secret_value(SecretId = secret_id)
            postgres_secret =  json.loads(response['SecretString'])
    
        conn = psycopg2.connect( host=postgres_secret['host'], user=postgres_secret['user'], password=postgres_secret['password'], dbname=postgres_secret['database'])
    except:
        display("Unexpected error:{}".format(sys.exc_info()[0]))
        raise
    return conn
def get_sql_server_connection(sql_server_secret):
    try:
        if isinstance(sql_server_secret, dict):
            None
        else:
            secret_id = sql_server_secret[0]
            region_name = sql_server_secret[1]
            client = boto3.client(
                service_name='secretsmanager',
                region_name=region_name
            )
            response = client.get_secret_value(SecretId = secret_id)
            sql_server_secret =  json.loads(response['SecretString'])
        connection_string = 'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={};DATABASE={};UID={};PWD={}'.format(sql_server_secret['host'],sql_server_secret['database'],sql_server_secret['user'],sql_server_secret['password'])
        conn = pyodbc.connect(connection_string)
    except:
        display("Unexpected error:{}".format(sys.exc_info()[0]))
        raise
    return conn
def get_table_definition(sql_server_connection, table_name):
    
    
    cursor = sql_server_connection.cursor()
    if '.' in table_name:
        if table_name not in ["{}.{}".format(x[1],x[2]).lower() for x in cursor.tables()]:
            cursor.close()
            raise ValueError("\nNo table called: '{}', please correct the table name and try again".format(table_name))
        info_table = [table_name]
    else:
        info_table = ["{}.{}".format(x[1],x[2]) for x in cursor.tables(table=table_name)]
        if info_table == []:
            cursor.close()
            raise ValueError("\nNo table called: '{}', please correct the table name and try again".format(table_name))
        elif len(info_table) > 1: 
            print(info_table)
            cursor.close()
            raise ValueError('\nthere are two tables with that name, please specify schema name\nNames:\n{}'.format('\n'.join(info_table)))
    info_table = info_table[0]
    info_columns = {'datecreated': '','int identity': ''}
    for x in cursor.columns(schema=info_table.split('.')[0], table=info_table.split('.')[1]).fetchall():
        info_columns['datecreated'] = x[3].lower() if x[3].lower() == 'datecreated' else info_columns['datecreated']
        info_columns['int identity'] = x[3].lower() if x[5].lower() == 'int identity' else info_columns['int identity']
    cursor.close()
    return info_columns, info_table.lower()
def check_if_exists_on_pg(pg_connection, table_name, pg_table, columns:dict):


    display('checking if table exists on postgres:')
    cursor = pg_connection.cursor()
    cursor.execute("select exists(select * from information_schema.tables where CONCAT(table_schema,'.',table_name)=%s)", (pg_table,))
    if cursor.fetchone()[0]:
        display('pg table: exists')
        cursor.close()
        cursor = pg_connection.cursor()
        cursor.execute("select column_name from information_schema.columns where CONCAT(table_schema,'.',table_name)=%s", (pg_table,))
        pg_columns = [x[0] for x in cursor.fetchall()]
        if (columns['datecreated'] == '' or columns['datecreated'] in pg_columns) and (columns['int identity'] == '' or columns['int identity'] in pg_columns) and (columns['datecreated'] != '' or columns['int identity'] != ''):
            display('pg columns: exists')
            return 0
        elif (columns['datecreated'] == '' or columns['int identity'] == ''):
            return 0
        else:
            cursor.close()
            raise ValueError("\columns '{}' does not exists in postgres, please check manually.".format(columns))
    else:
        cursor.close()
        raise ValueError(f"\nTable '{table_name}' does not exists in postgres, please check manually.")
def calculate_total_gap(sql_server_connection, pg_connection, table_name, pg_table):
    sql_cursor = sql_server_connection.cursor()
    pg_cursor = pg_connection.cursor()

    sql_query = 'SELECT COUNT(*) cu FROM {t} WITH(NOLOCK);'.format(t = table_name)
    pg_query =  'SELECT COUNT(*) cu FROM {t};'.format( t = pg_table)

    print(sql_query)
    print(pg_query)

    sql_cursor.execute(sql_query)
    pg_cursor.execute(pg_query)
    
    sql_count = sql_cursor.fetchall()[0][0]
    pg_count = pg_cursor.fetchall()[0][0]
    
    gap = '\nsql: {}.\npg: {}\ngap: {}\n'.format(sql_count, pg_count, sql_count-pg_count)

    return gap
def calculate_datecreated(sql_server_connection, pg_connection, table_name, pg_table, debug = 0):
    
    sql_cursor = sql_server_connection.cursor()
    pg_cursor = pg_connection.cursor()
    
    sql_query = 'SELECT {c} AS dt, COUNT(*) cu FROM {t} WITH(NOLOCK) GROUP BY {c} ORDER BY {c} DESC;'.format(c = 'CAST(datecreated as date)', t = table_name)
    pg_query = 'SELECT {c} AS dt, COUNT(*) cu FROM {t} GROUP BY {c} ORDER BY {c} DESC;'.format(c = 'CAST(datecreated as date)', t = pg_table)

    display('Running query against SQL Server.',f"query: {sql_query}")
    sql_cursor.execute(sql_query)
    sql_dict = {k:v for k,v in sql_cursor.fetchall()}
    sql_cursor.close()
    print(sql_dict) if debug == 2 else None

    display('Running query against Postgres.',f"query: {pg_query}")
    pg_cursor.execute(pg_query)
    pg_dict = {k:v for k,v in pg_cursor.fetchall()}
    pg_cursor.close()
    print(pg_dict) if debug == 2 else None

    gap = get_gap(sql_dict, pg_dict, 'datecreated', debug = debug)
    
    return gap
def identity_for_datecreated(sql_server_connection, table_name, column, specific_values:dict, debug = 0):

    
    where = "','".join([str(val[1]) for dic in specific_values['gap_on_pg'] for val in dic.items() if val[0] == 'Row'])
    query = 'SELECT DISTINCT {c}/100000 FROM {t} WITH(NOLOCK) WHERE CAST(datecreated AS DATE) IN (\'{w}\')'.format(c = column, t = table_name, w = where)
    cursor = sql_server_connection.cursor()
    cursor.execute(query)
    t = cursor.fetchall()
    specific_values['gap_on_pg'] = []
    # specific_values['gap_on_pg'].append({"Row":v[0] for v in cursor.fetchall()})
    [specific_values['gap_on_pg'].append({"Row":v[0]}) for v in t]

    print(specific_values) if debug == 2 else None
    return specific_values
def calculate_identity(sql_server_connection, pg_connection, table_name, column, pg_table, specific_values:dict = None, divide:int = 100000, debug = 0):
    
    sql_cursor = sql_server_connection.cursor()
    pg_cursor = pg_connection.cursor()

    print(specific_values) if debug == 2 else None

    if specific_values is not None:
        # parse the gap values
        where = ",".join([str(val[1]) for dic in specific_values['gap_on_pg'] for val in dic.items() if val[0] == 'Row'])
        sql_query = 'SELECT {c}, COUNT(*) cu FROM {t} WITH(NOLOCK) WHERE {oc} IN ({w}) GROUP BY {c} ORDER BY {c} DESC;'.format(c = f"{column}/{divide}", t = table_name, w = where, oc = f"{column}/{divide*10}")
        pg_query = 'SELECT {c}, COUNT(*) cu FROM {t} WHERE {oc} IN ({w}) GROUP BY {c} ORDER BY {c} DESC;'.format(c = f"{column}::int/{divide}", t = pg_table, w = where, oc = f"{column}::int/{divide*10}")
    
    else:
        sql_query = 'SELECT {c}, COUNT(*) cu FROM {t} WITH(NOLOCK) GROUP BY {c} ORDER BY {c} DESC;'.format(c = f"{column}/{divide}", t = table_name)
        pg_query = 'SELECT {c}, COUNT(*) cu FROM {t} GROUP BY {c} ORDER BY {c} DESC;'.format(c = f"{column}::int/{divide}", t = pg_table)
        
    display(f'Running with division value of {divide}','Running query against SQL Server.',f"query: {sql_query}")
    sql_cursor.execute(sql_query)
    sql_dict = {k:v for k,v in sql_cursor.fetchall()}
    sql_cursor.close()
    print(sql_dict) if debug == 2 else None

    display('Running query against Postgres.',f"query: {pg_query}")
    pg_cursor.execute(pg_query)
    pg_dict = {k:v for k,v in pg_cursor.fetchall()}
    pg_cursor.close()
    print(pg_dict) if debug == 2 else None

    gap = get_gap(sql_dict, pg_dict, column, divide, debug)
    if specific_values is not None:
        gap['missing_rows'] += specific_values['missing_rows']

    if gap['gap_on_pg'] == []:
        return gap
    else:
        return calculate_identity(sql_server_connection, pg_connection, table_name, column, pg_table, gap ,divide//10, debug = debug)
def get_gap(sql_dict, pg_dict, column, divide = 0,  debug = 0):

    display('Calculating Gap')
    gap = {"missing_rows": [], "gap_on_pg": [], "gap_on_mssql": []}
    
    # calculate missing rows on pg
    sql_set = set(sql_dict.keys())
    pg_set = set(pg_dict.keys())
    missing_rows_on_pg = sql_set.difference(pg_set)
    # calculate difference rows
    gap["missing_rows"].append({"column": column, "divide": divide, "values": []})
    for i in missing_rows_on_pg:
        sql_dict.pop(i)
        gap["missing_rows"][-1]["values"].append(f"'{i}'" if type(i) == 'datetime.date' else i)


    for key,value in sql_dict.items():
        if pg_dict[key] < value:
            gap["gap_on_pg"].append({"Row": key,"SQL value":value, "PG value": pg_dict[key]})
        elif pg_dict[key] > value:
            gap["gap_on_mssql"].append({"Row": key,"SQL value":value, "PG value": pg_dict[key]})
        else:
            None
    print(gap) if debug == 2 else None
    display("Done",'')
    return gap
def building_queries(sql_db, table_name, pg_table, gap):

    # no pg gap
    where =' OR '.join(["{} IN ({})".format(''.join([i['column'], '/'+str(i['divide']) if i['divide'] > 1 else '']), ", ".join(str(l) if type(l)==type(int()) else "'{}'".format(str(l)) for l in i['values'])) for i in gap['missing_rows'] if i['values'] != []])
    query = f'--SQL Server Side\nUSE [{sql_db}]\nGO\n'
    query += f'--DROP TABLE {table_name}_temph\n'
    query += 'SELECT\t*\nINTO\t{t}_temph\nFROM\t{t}\nWHERE\t{w};'.format(t = table_name, w = where)
    query += '\n\n--INSERT Command\nINSERT INTO\t{pgt}\nFROM\t{t}_temph\nON CONFLICT DO NOTHING;'.format(pgt = pg_table, t = table_name)
    return query

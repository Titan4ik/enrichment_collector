import asyncio
import asyncpg
from pprint import pprint
import ast
import define_data_type as DTF

IS_LOG = True

class Cache:
    def __init__(self):
        self._results = {}

    async def execute(self, conn, table, param, value):
        sql_request = f"SELECT * FROM {table} WHERE {param}='{value}'"
        try:
            return self._results[sql_request]
        except KeyError:
            res = await conn.fetch(sql_request)
            await insert_into_select_request_log(table, param, str(value))
            self._results[sql_request] = res
            return res
        
            
async def get_connection():

    return await asyncpg.connect(user=user, password=password,
                                 database=database, host=host)
                                 
def connection(sql_func):
    async def wraper(*args, **kwargs):
        conn = await get_connection()
        try:
            return await sql_func(conn, *args, **kwargs)
        finally:
            await conn.close()
    return wraper

@connection                              
async def get_relationship(conn):
    values = await conn.fetch('''SELECT * FROM relationship_in_tables''')
    retval = {}
    for value in values:
        try:
            retval[value['table1_name']].append([value['table2_name'], value['column_from_table1'], value['column_from_table2']])
        except KeyError:
            retval[value['table1_name']] = [[value['table2_name'], value['column_from_table1'], value['column_from_table2']]]
        try:
            retval[value['table2_name']].append([value['table1_name'], value['column_from_table2'], value['column_from_table1']])
        except KeyError:
            retval[value['table2_name']] = [[value['table1_name'], value['column_from_table2'], value['column_from_table1']]]
    return retval

@connection
async def insert_into_select_request_log(conn, table, param, value):
    if not IS_LOG:
        return
    await conn.execute(
        "INSERT INTO select_request_log (table_name, column_name, column_value, request_time) "
        "VALUES($1, $2, $3, current_timestamp)",
        table, param, value
    )

@connection 
async def get_info(conn, table_name, param_name, param_value):
    tree = await get_relationship()
    info = {}
    paths = {}
    current_tables = [(table_name,param_name,param_value)]
    cache = Cache()
    while current_tables:
        table, param, value = current_tables.pop(0)
        if table not in paths:
            paths[table] = set()
        try:
            datas = await cache.execute(conn, table, param, value)
            # datas = await conn.fetch(f"SELECT * FROM {table} WHERE {param}='{value}'")
        except Exception as e:
            continue
        if not datas:
            continue
        try:
            _ = info[table]
        except Exception:
            info[table] = set()
        is_added = False
        for data in datas:
            if data not in info[table]:
                is_added = True
                info[table].add(data)
        if not is_added:
            continue
        next_tables = tree.get(table)
        if next_tables is None:
            continue
        for (next_table, prev_param, next_param) in next_tables:
            if not(next_table in paths and table in paths[next_table]):
                paths[table].add(next_table)
            for data in datas:
                current_tables.append(
                    [next_table, next_param, data[prev_param]]
                )
    print(f'Был пройден следующий путь начиная с {table_name}')
    pprint(paths)
    return info


async def find_related_tables(table_name):
    tree = await get_relationship()
    related_tables = {table_name}
    current_tables = [table_name]
    while current_tables:
        table = current_tables.pop(0)
        next_tables = tree.get(table)
        if next_tables is None:
            continue
        for (next_table, prev_param, next_param) in next_tables:
            if next_table not in related_tables:
                related_tables.add(next_table)
                current_tables.append(next_table)
    return related_tables

@connection
async def get_tables(conn):
    sql = (
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema='public' "
        "order by table_name"
    )
    rows = await conn.fetch(sql)
    tables = {}
    ignore_tables = [
        'relationship_in_tables', 'type_columns_in_tables'
    ]
    for row in rows:
        if row['table_name'] in ignore_tables:
            continue
        try:
            tables[row['table_name']].append([row['column_name'], row['data_type']])
        except KeyError:
            tables[row['table_name']] = [[row['column_name'], row['data_type']]]
    return tables

@connection
async def analyze_relationship(conn, curr_table):
    tables = await get_tables()
    curr_columns = tables[curr_table]
    del tables[curr_table]
    for name, columns in tables.items():
        for column in columns:
            for curr_column in curr_columns:
                if curr_column[1] == column[1]:
                    similar_procent = await analyze_two_columns(curr_table, curr_column[0], name, column[0])
                    if similar_procent:
                        await insert_relationship(curr_table, curr_column[0], name, column[0], similar_procent)
    sql = (
        "SELECT table_name, column_name, column_type "
        "FROM type_columns_in_tables "
        f"WHERE column_type in (SELECT column_type FROM type_columns_in_tables WHERE table_name='{curr_table}')"
    )
    rows = await conn.fetch(sql)
    curr_columns = {}
    columns_type = {}
    for row in rows:
        if row['table_name'] == curr_table:
            curr_columns[row['column_type']] = row['column_name']
            continue
        try:
            columns_type[row['column_type']].append([row['table_name'], row['column_name']])
        except KeyError:
            columns_type[row['column_type']] = [[row['table_name'], row['column_name']]]
        
    for type_, column_name1 in curr_columns.items():
        data = columns_type.get(type_)
        if data is None:
            continue
        for table2, column_name2 in data:
            await insert_relationship(curr_table, column_name1, table2, column_name2)


@connection
async def insert_relationship(conn, table1, column1, table2, column2, similar_procent=0):
    rows = await conn.fetch(
        "SELECT * FROM relationship_in_tables "
        f"WHERE table1_name='{table2}' and column_from_table1='{column2}' and table2_name='{table1}' and column_from_table2='{column1}'"
    )
    if rows:
        return
    await conn.execute(
        "INSERT INTO relationship_in_tables (table1_name, column_from_table1, table2_name, column_from_table2) "
        "VALUES($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        table1, column1, table2, column2
    )
    print(table1, column1, table2, column2, f"[similar = {similar_procent*100}%]")

@connection
async def analyze_two_columns(conn, table1, column1, table2, column2):
    sql_full = (
        f"SELECT {table1}.{column1} AS col1, {table2}.{column2} AS col2 "
        f"FROM {table1} "
        f"FULL JOIN {table2} "
        f"ON {table1}.{column1}={table2}.{column2}"
    )
    res_full = await conn.fetch(sql_full)
    sql_inner = sql_full.replace('FULL JOIN', 'INNER JOIN')
    res_inner = await conn.fetch(sql_inner)
    if len(res_full) > 0:
        # print(res_full)
        return len(res_inner) / len(res_full)



@connection
async def detect_column_type(conn, table):
    rows = await conn.fetch(f'SELECT * FROM {table}')
    types = {}
    for row in rows:
        for param_name, param_value in row.items():
            param_value = str(param_value)
            for assumption in DTF.detect_type(param_value):
                try:
                    types[param_name][assumption] += 1
                except KeyError:
                    types[param_name] = {assumption: 1}
    pprint(types)
    for column, types in types.items():
        for type_name in types:
            await insert_type_columns_in_tables(table, column, type_name)

@connection
async def insert_type_columns_in_tables(conn, table, column, type_name):
    await conn.execute(
        "INSERT INTO type_columns_in_tables(table_name, column_name, column_type) VALUES($1, $2, $3) ON CONFLICT DO NOTHING",
        table, column, type_name
    )

async def prepare():
    for table in await get_tables():
        await detect_column_type(table)
    for table in await get_tables():
        await analyze_relationship(table)

async def main():
    # await prepare()
    print(f"Найденная информация")
    pprint(await get_info('vk_posts', 'post_url', 'url1'))
    # table_name = 'peoples'
    # print(f'Связанные таблицы для {table_name}')
    # pprint(await find_related_tables(table_name))
    # print('Список всех таблиц')
    # pprint(await get_tables())
    
    


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

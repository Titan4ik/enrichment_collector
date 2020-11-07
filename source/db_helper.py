import cx_Oracle
import log
import define_data_type as DTF


class Cache:
    def __init__(self):
        self._results = {}

    def execute(self, conn, table, param, value):
        sql_request = f"SELECT * FROM {table} WHERE {param}='{value}'"
        try:
            return self._results[sql_request]
        except KeyError:
            with conn.cursor() as cursor:
                res = cursor.execute(sql_request)
            self._results[sql_request] = res
            return res


def __connection() -> tuple:
    return "SYS", cx_Oracle.connect(
        "SYSDBA",
        "",
        "localhost:1521/xe",
        encoding="UTF-8",
        mode=cx_Oracle.SYSDBA,
    )


def connection(*, commit=False):
    def wrapper(func):
        def wrapper_func(*args):
            conn = None
            try:
                user_name, conn = __connection()
                return func(conn, user_name, *args)
            # except Exception as e:
            #     log.error(e)
            finally:
                if conn is not None:
                    if commit:
                        conn.commit()
                    conn.close()

        return wrapper_func

    return wrapper


@connection(commit=True)
def init_tables(conn, user_name):
    sqls = [
        f"""\
        CREATE TABLE relationship_in_tables(
            table1_name VARCHAR2(64),
            column_from_table1 VARCHAR2(64),
            table2_name VARCHAR2(64),
            column_from_table2 VARCHAR2(64),
            primary key(table1_name, column_from_table1, table2_name, column_from_table2)
        )""",
        f"""\
        CREATE TABLE type_columns_in_tables(
            table_name VARCHAR2(64),
            column_name VARCHAR2(64),
            column_type VARCHAR2(64),
            primary key(table_name, column_name, column_type)
        )""",
        f"""\
        CREATE TABLE enrichment_tables(
            table_name VARCHAR2(64),
            column_name VARCHAR2(64),
            data_type VARCHAR2(64),
            column_id INTEGER,
            primary key(table_name, column_name)
        )""",
    ]
    with conn.cursor() as cursor:
        for sql in sqls:
            sql = sql.replace("        ", "")
            try:
                print(sql)
                cursor.execute(sql)

            except Exception as e:
                print("error")
                print(e)
            else:
                print("good")


@connection()
def get_relationship(conn, user_name):
    retval = {}
    with conn.cursor() as cursor:
        for row in cursor.execute(
            f"SELECT table1_name, column_from_table1, table2_name, column_from_table2 FROM relationship_in_tables"
        ):
            try:
                retval[row[0]].add((row[2], row[1], row[3]))
            except KeyError:
                retval[row[0]] = set((row[2], row[1], row[3]))
            try:
                retval[row[2]].add((row[0], row[3], row[1]))
            except KeyError:
                retval[row[2]] = set((row[0], row[3], row[1]))
    return retval


# @connection()
# def insert_into_select_request_log(conn, user_name, table, param, value):
#     if not IS_LOG:
#         return
#      conn.execute(
#         "INSERT INTO select_request_log (table_name, column_name, column_value, request_time) "
#         "VALUES($1, $2, $3, current_timestamp)",
#         table, param, value
#     )


@connection()
def get_info(conn, user_name, table_name, param_name, param_value):
    tree = get_relationship()
    info = {}
    paths = {}
    current_tables = [(table_name, param_name, param_value)]
    cache = Cache()
    while current_tables:
        table, param, value = current_tables.pop(0)
        if table not in paths:
            paths[table] = set()
        try:
            datas = cache.execute(conn, user_name, table, param, value)
            # datas =  conn.fetch(f"SELECT * FROM {table} WHERE {param}='{value}'")
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
            if not (next_table in paths and table in paths[next_table]):
                paths[table].add(next_table)
            for data in datas:
                current_tables.append([next_table, next_param, data[prev_param]])
    print(f"Был пройден следующий путь начиная с {table_name}")
    return info


@connection()
def get_tables(conn, user_name):
    sql = (
        "SELECT table_name, column_name, data_type "
        f"FROM enrichment_tables "
        "order by table_name"
    )
    tables = {}
    with conn.cursor() as cursor:
        for row in cursor.execute(sql):
            try:
                tables[row[0]].append([row[1], row[2]])
            except KeyError:
                tables[row[0]] = [[row[1], row[2]]]
    return tables


@connection()
def analyze_relationship(
    conn, user_name, tables: list, curr_table: str, curr_columns: list
):
    # делает проход по всем таблицам и пытается найти связь на основе содержимого
    for name, columns in tables.items():
        if name == curr_table:
            continue
        for column in columns:
            for curr_column in curr_columns:
                if curr_column[1] == column[1]:
                    similar_procent = analyze_two_columns(
                        curr_table, curr_column[0], name, column[0]
                    )
                    if similar_procent:
                        insert_relationship(
                            curr_table, curr_column[0], name, column[0], similar_procent
                        )
    # получаем все таблицы у которых колонки имеют похожий тип на тот, который в исследуемой таблице, например колонка телефона
    sql = (
        "SELECT table_name, column_name, column_type "
        f"FROM type_columns_in_tables "
        f"WHERE column_type in (SELECT column_type FROM type_columns_in_tables WHERE table_name='{curr_table}')"
    )

    curr_columns = {}
    columns_type = {}
    with conn.cursor() as cursor:
        for row in cursor.execute(sql):
            if row[0] == curr_table:
                curr_columns[row[2]] = row[1]
                continue
            try:
                columns_type[row[2]].append([row[0], row[1]])
            except KeyError:
                columns_type[row[2]] = [[row[0], row[1]]]

    for type_, column_name1 in curr_columns.items():
        data = columns_type.get(type_)
        if data is None:
            continue
        for table2, column_name2 in data:
            insert_relationship(curr_table, column_name1, table2, column_name2)


@connection(commit=True)
def insert_relationship(
    conn, user_name, table1, column1, table2, column2, similar_procent=0
):
    sql = (
        f"SELECT * FROM relationship_in_tables "
        f"WHERE "
        f"table1_name='{table1}' and column_from_table1='{column1}' and table2_name='{table2}' and column_from_table2='{column2}' "
        "OR "
        f"table1_name='{table2}' and column_from_table1='{column2}' and table2_name='{table1}' and column_from_table2='{column1}'"
    )
    with conn.cursor() as cursor:
        for row in cursor.execute(sql):
            return
    with conn.cursor() as cursor:
        sql = (
            f"INSERT INTO relationship_in_tables (table1_name, column_from_table1, table2_name, column_from_table2) "
            "VALUES(:1, :2, :3, :4)"
        )
        cursor.execute(sql, [table1, column1, table2, column2])
    # print(table1, column1, table2, column2, f"[similar = {similar_procent*100}%]")


@connection()
def analyze_two_columns(conn, user_name, table1, column1, table2, column2):
    sql_full = (
        f"SELECT {table1}.{column1} AS col1, {table2}.{column2} AS col2 "
        f"FROM {table1} "
        f"FULL JOIN {table2} "
        f"ON {table1}.{column1}={table2}.{column2}"
    )
    sql_inner = sql_full.replace("FULL JOIN", "INNER JOIN")
    with conn.cursor() as cursor:
        cursor.execute(sql_full)
        res_full = cursor.fetchall()
        cursor.execute(sql_inner)
        res_inner = cursor.fetchall()

    if len(res_full) > 0:
        # print(res_full)
        return len(res_inner) / len(res_full)


@connection()
def detect_column_type(conn, user_name, table):
    types = {}
    with conn.cursor() as cursor:
        rows = cursor.execute(f"SELECT * FROM {table}")
        col_names = [row[0] for row in cursor.description]
        for row in rows:
            for param_name, param_value in zip(col_names, row):
                param_value = str(param_value)
                for assumption in DTF.detect_type(param_value):
                    try:
                        types[param_name][assumption] += 1
                    except KeyError:
                        types[param_name] = {assumption: 1}
    for column, types in types.items():
        for type_name in types:
            insert_type_columns_in_tables(table, column, type_name)


@connection(commit=True)
def insert_type_columns_in_tables(conn, user_name, table, column, type_name):
    with conn.cursor() as cursor:
        sql = (
            "INSERT /*+ ignore_row_on_dupkey_index (type_columns_in_tables(table_name, column_name, column_type)) */ "
            f"INTO type_columns_in_tables(table_name, column_name, column_type) VALUES(:1, :2, :3)"
        )
        cursor.execute(sql, [table, column, type_name])


@connection(commit=True)
def insert_data_in_table(conn, user_name, table, rows, columns=None):
    if columns is None:
        with conn.cursor() as cursor:
            columns = [
                x[0]
                for x in cursor.execute(
                    f"SELECT column_name FROM enrichment_tables WHERE table_name='{table}' ORDER BY COLUMN_ID"
                )
            ]
    columns_str = ", ".join([str(x) for x in columns])
    columns_num = ", ".join([f":{i+1}" for i, _ in enumerate(columns)])
    with conn.cursor() as cursor:
        for row in rows:
            try:
                cursor.execute(
                    f"INSERT INTO {table} ({columns_str}) values ({columns_num})", row
                )
            except Exception as e:
                print(e)
            


@connection(commit=True)
def insert_info_about_table(conn, user_name, table, schema):
    rows = [
        (table, column_name, data_type, i)
        for i, (column_name, data_type) in enumerate(schema)
    ]
    content = ",\n".join(
        [
            f"\t{column_name} {data_type}"
            for column_name, data_type in schema
        ]
    )
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT table_name FROM enrichment_tables WHERE table_name='{table}'"
        )
        if not cursor.fetchone():
            sql = f"CREATE TABLE {table}(\n{content}\n)"
            log.debug(sql)
            cursor.execute(sql)
            cursor.executemany(
                "INSERT INTO enrichment_tables (table_name, column_name, data_type, column_id) values (:1, :2, :3, :4)",
                rows,
            )

@connection(commit=True)
def delete_table(conn, user_name, table):
    with conn.cursor() as cursor:
        cursor.execute(
            f"DELETE FROM enrichment_tables WHERE table_name='{table}'"
        )
        cursor.execute(
            f"DROP TABLE {table}"
        )

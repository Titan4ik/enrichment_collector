import csv
from main import connection


@connection
async def insert_new_data(conn, table_name, headers, values):
    headers_str = ','.join([str(x) for x in headers])
    values_str = ', '.join([f'${i}' for i, _ in enumerate(headers)])
    await conn.execute(
        f"INSERT INTO {table_name} (headers_str) "
        "VALUES($1, $2, $3)",
        values
    )

@connection
async def create_table(conn, table_name, headers):
    sqls = [
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
    ]
    for header in headers:
        sqls.append(f'{header} VARCHAR(256),')
    sqls.append(");")
    await conn.execute(
        '\n'.join(sqls)
    )


async def parse(path, table_name):
    with open(path, 'r', encoding='utf-8') as fin:
        reader = csv.DictReader(fin)
        headers = reader.fieldnames
        await create_table(table_name, headers)
        await insert_new_data(table_name, headers, [line for line in reader])


async def main():
    path = ''
    table_name = ''
    await parse(path,table_name)



loop = asyncio.get_event_loop()
loop.run_until_complete(main())

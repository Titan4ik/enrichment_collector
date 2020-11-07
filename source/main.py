import db_helper
from pprint import pprint
from datetime import datetime
from prettytable import PrettyTable
import csv
import json


def find_related_tables(table_name):
    tree = db_helper.get_relationship()
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


def prepare():
    db_helper.init_tables()
    tables = db_helper.get_tables()
    for table in tables:
        db_helper.detect_column_type(table)
    for table, columns in tables.items():
        db_helper.analyze_relationship(tables, table, columns)


def main():
    
    print(f"Найденная информация")
    # pprint(get_info("vk_posts", "post_url", "url1"))
    table_name = "address_and_payment_of_inspections"
    # print(f"Связанные таблицы для {table_name}")
    # pprint(find_related_tables(table_name))
    print("Список всех таблиц")
    pprint(db_helper.get_tables())


# def insert_test_data():
#     db_helper.insert_data_in_table(
#         "peoples",
#         ["name", "telephone", "email", "date_of_birth"],
#         [
#             ("aleks", "79992254552", "sasha@mail.ru", datetime(1998, 4, 29)),
#             ("vasya", "79992254444", "vasya@mail.ru", datetime(1998, 6, 26)),
#             ("katya", "79992254441", "katya@mail.ru", datetime(1998, 8, 19)),
#             ("lena", "79992253333", "lena@mail.ru", datetime(1998, 1, 9)),
#         ],
#     )
#     db_helper.insert_data_in_table(
#         "user_in_social",
#         ["telephone", "facebook_id", "vk_id"],
#         [
#             ("79992254552", 0, 0),
#             ("79992254444", 1, 1),
#             ("79992254441", 3, 10),
#             ("79992253333", 4, 222),
#         ],
#     )
#     db_helper.insert_data_in_table(
#         "vk_posts",
#         ["vk_id", "post_url"],
#         [
#             (0, "url1"),
#             (0, "url2"),
#             (0, "url3"),
#             (0, "url5"),
#             (1, "url11"),
#             (1, "url12"),
#             (1, "url13"),
#             (1, "url15"),
#             (5, "url131"),
#             (5, "url151"),
#             (3, "url1312"),
#             (3, "url1512"),
#         ],
#     )
#     db_helper.insert_data_in_table(
#         "mail",
#         ["email_from", "email_to", "msg"],
#         [
#             ("sasha@mail.ru", "vasya@mail.ru", "hello"),
#             ("vasya@mail.ru", "sasha@mail.ru", "hello"),
#             ("sasha@mail.ru", "vasya@mail.ru", "go in dota2"),
#             ("vasya@mail.ru", "sasha@mail.ru", "go"),
#             ("hehi@mail.ru", "don@mail.ru", "go in weesssese"),
#             ("don@mail.ru", "katya@mail.ru", "go"),
#         ],
#     )


def load_csv(path_to_file: str):
    with open(path_to_file, "r", newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';', quotechar='|')
        header = None
        rows = []
        for line in reader:
            if header is None:
                header = line
            else:
                rows.append(line)
    return header, rows


def load_data_from_csv(table_name: str, path_to_file: str):
    header, rows = load_csv(path_to_file)
    db_helper.insert_data_in_table(table_name, rows)

def print_table(table_name, header, rows):
    columns = len(header)
    table = PrettyTable(header)
    td_data = rows[:]
    for row in rows:
        table.add_row(row)
    with open(f'{table_name}.txt', 'w', encoding='utf-8') as f:
        f.writelines(table.get_string())
    print(table) 

def load_schema(schema_name):
    with open(f'.\schema_for_enrichment_table\{schema_name}.json') as json_file:
        data = json.load(json_file)
    db_helper.insert_info_about_table(
        data['table_name'],
        data['schema'],
    )


if __name__ == "__main__":
    prepare()

    # insert_test_data
    # print_table(*load_csv('1.csv'))
    # print_table(*load_csv('2.csv'))
    # print_table('structure',*load_csv('3.csv'))
    # print_table('data',*load_csv('4.csv'))
    db_helper.delete_table('address_and_payment_of_inspections')
    load_schema('address_and_payment_of_inspections')
    load_data_from_csv('address_and_payment_of_inspections', '4.csv')
    main()
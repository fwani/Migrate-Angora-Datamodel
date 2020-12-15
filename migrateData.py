import argparse
import csv
import json
import os
import pandas as pd
import psycopg2
import re
import string
import xlrd

from makeTableSchema import transform_column_name


A_TO_Z_STRING = {v: i for i, v in enumerate(string.printable[36:62])}


def make_excel_range(cells):
    if not cells:
        return None, None
    grep = re.search(r'([A-Za-z]+)(\d+)\:([A-Za-z]+)(\d+)', cells)
    grep = grep.groups()
    # print(grep)

    cols = range(A_TO_Z_STRING[grep[0]], A_TO_Z_STRING[grep[2]]+1)
    rows = range(int(grep[1])-1, int(grep[3]))
    # print(cols, rows)
    return cols, rows


def migrate(cur, path: str, table_name: str, options=None):
    if os.path.isfile(path):
        base_query = 'INSERT INTO {0}({1}) VALUES {2};'
        if path.endswith('csv'):
            with open(path, 'r', encoding='utf-8-sig') as fr:
                reader = csv.reader(fr)
                header = next(reader)

                value_format = "(" + ', '.join(['%s'] * len(header)) + ")"
                all_rows = []
                for row in reader:
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))

                cnt = 0
                trans_header = []
                for h in header:
                    if not h.strip():
                        h = '_c{}'.format(cnt)
                        cnt += 1
                    trans_header.append('"{}"'.format(transform_column_name(h)))
                print(table_name, trans_header)

                cur.execute(base_query.format(table_name, ','.join(trans_header), ','.join(all_rows)))
                # cur.commit()
        else:
            # print(path, options)
            sheet_name = options.get('sheet_name', None)
            if not sheet_name:
                sheet_name = 'Sheet1'
            cells = options.get('cells', None)
            if not cells:
                cells = None
            else:
                cells = cells.upper()
            # print(sheet_name, cells)

            workbook = xlrd.open_workbook(path)
            worksheet = workbook.sheet_by_name(sheet_name)
            cols, rows = make_excel_range(cells)
            if cols and rows:
                data = []
                for row in rows:
                    tmp = []
                    for col in cols:
                        # print(row, col)
                        tmp.append(worksheet.cell_value(row, col))
                    data.append(tmp)
                # print(data)
                header = data[0]
                value_format = "(" + ', '.join(['%s'] * len(header)) + ")"
                all_rows = []
                for row in data[1:]:
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))
                # print(header, all_rows)


            else:
                # print(worksheet.row_values(12))

                df = pd.read_excel(path, sheet_name=sheet_name)
                # print(df)
                header = df.columns
                value_format = "(" + ', '.join(['%s'] * len(header)) + ")"
                all_rows = []
                for i, row in df.iterrows():
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))

            cnt = 0
            trans_header = []
            for h in header:
                if not h:
                    h = '_c{}'.format(cnt)
                    cnt += 1
                trans_header.append('"{}"'.format(transform_column_name(h)))
            print(table_name, trans_header)

            cur.execute(base_query.format(table_name, ','.join(trans_header), ','.join(all_rows)))


def run(args):
    dbname = args.database
    print("Connect Database [{}]".format(dbname))
    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user, password=args.password, dbname=dbname)
    cur = conn.cursor()
    conn.autocommit = True
    cur.execute(open('create_table.sql', 'r').read())
    df = pd.read_csv(args.input, encoding=args.encoding, sep=args.delimiter)
    # df = df.drop('table_name', axis=1)
    cnt = 0
    for i, v in df[['DATASET', 'table_name']].iterrows():
        cnt += 1
        # print(json.loads(v[0]))
        v[0] = json.loads(v[0])
        path = os.path.join(args.prefix, re.search(r'([^"]+)', v[0]['path']).group(1).rsplit('/')[-1])
        table_name = v[1]
        # print(path, table_name)
        migrate(cur, path, table_name, v[0]['option'])
    # migrate(cur, './input.csv', 'test')

    cur.close()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Change dataset of Angora Metastore from origin data-source to new.")
    parser.add_argument('-i', '--input',
                        default='./datamodel_w_table_name.csv',
                        type=str,
                        required=True,
                        help='Path of input csv file.')
    parser.add_argument('-e', '--encoding',
                        default='utf-8',
                        type=str,
                        required=False,
                        help='Encoding of input file.')
    parser.add_argument('-d', '--delimiter',
                        default=',',
                        type=str,
                        required=False,
                        help='Delimiter of input csv file.')
    parser.add_argument('--prefix',
                        default='data',
                        type=str,
                        required=False,
                        help='The path of data files.')
    parser.add_argument('--host',
                        required=True,
                        type=str,
                        help='PostgreSQL host')
    parser.add_argument('--port',
                        required=True,
                        type=str,
                        help='PostgreSQL port')
    parser.add_argument('--user',
                        required=True,
                        type=str,
                        help='PostgreSQL user')
    parser.add_argument('--password',
                        required=True,
                        type=str,
                        help='PostgreSQL password')
    parser.add_argument('--database',
                        required=True,
                        type=str,
                        help='PostgreSQL database name')

    print(parser.parse_args())

    run(parser.parse_args())

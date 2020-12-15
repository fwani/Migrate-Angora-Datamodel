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


def get_header(header):
    trans_header = []
    cnt = 0
    for h in header:
        if not h:
            h = '_c{}'.format(cnt)
            cnt += 1
        trans_header.append('"{}"'.format(transform_column_name(h)))
    return trans_header


def get_real_index(cur, table_name, header):
    real_header_index = []
    for idx, h in enumerate(header):
        try:
            cur.execute('SELECT {} FROM "{}"'.format(h, table_name))
            real_header_index.append(idx)
        except:
            pass
    return real_header_index


def migrate(cur, path: str, table_name: str, options=None):
    if os.path.isfile(path):
        base_query = 'INSERT INTO "{0}"({1}) VALUES {2};'
        if path.endswith('csv'):
            with open(path, 'r', encoding='utf-8-sig') as fr:
                reader = csv.reader(fr)
                header = get_header(next(reader))
                real_index = get_real_index(cur, table_name, header)

                value_format = "(" + ', '.join(['%s'] * len(real_index)) + ")"
                all_rows = []
                for row in reader:
                    row = [row[i] for i in real_index]
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))
        else:
            # print(path, options)
            sheet_name = options.get('sheet_name', None)
            if not sheet_name:
                sheet_name = None
            cells = options.get('cells', None)
            if not cells:
                cells = None
            else:
                cells = cells.upper()
            print(sheet_name, cells)

            workbook = xlrd.open_workbook(path)
            if sheet_name:
                worksheet = workbook.sheet_by_name(sheet_name)
            else:
                worksheet = workbook.sheet_by_index(0)
            cols, rows = make_excel_range(cells)
            print(cols, rows)
            if cols and rows:
                data = []
                for row in rows:
                    tmp = []
                    for col in cols:
                        # print(row, col)
                        # print(worksheet.cell_value(row, col))
                        tmp.append(worksheet.cell_value(row, col))
                    data.append(tmp)
                # print(data)
                header = get_header(data[0])
                real_index = get_real_index(cur, table_name, header)
                value_format = "(" + ', '.join(['%s'] * len(real_index)) + ")"
                all_rows = []
                for row in data[1:]:
                    row = [row[i] for i in real_index]
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))
                # print(header, all_rows)

            else:
                # print(worksheet.row_values(12))

                header = get_header(worksheet.row_values(0))
                real_index = get_real_index(cur, table_name, header)
                # header = next(data)
                # print(header)
                value_format = "(" + ', '.join(['%s'] * len(real_index)) + ")"
                all_rows = []
                for i in range(1, worksheet.nrows):
                    row = worksheet.row_values(i)
                    row = [row[i] for i in real_index]
                    all_rows.append(cur.mogrify(value_format, row).decode('utf-8'))
                # print(all_rows)

        cur.execute(base_query.format(table_name, ','.join([header[i] for i in real_index]), ','.join(all_rows)))


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

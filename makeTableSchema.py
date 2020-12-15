import argparse
import csv
import json
import pandas as pd
import re


def transform_table_name(name: str) -> str:
    """Change name that is started number

    :param name: str
        2019 국제선 국내선 화물량 차이
    :return:
        국제선 국내선 화물량 차이 2019
    """
    re_name = re.match(r'(^\d+\S+)(.*)', name)
    if re_name:
        re_name.group(2).strip() + '_' + re_name.group(1).strip()
    else:
        return name
    return name


def get_table_name(s: pd.Series) -> pd.Series:
    """To get table name from name of Datamodel of Angora

    :param s: pd.Series
        국제선 국내선 화물량 차이_2010_2018
        월별 도로교통사고사망자(15-19합)
    :return:
        국제선_국내선_화물량_차이_2010_2018
        월별_도로교통사고사망자_15_19합
    """
    s = s.replace(r'_', ' ', regex=True)
    s = s.apply(transform_table_name)
    s = s.replace(r'\s|,|\(|\)|\-|\~', '_', regex=True)
    s = s.replace(r'(^\d+|^_)', r'tab_\g<1>', regex=True)
    s = s.replace(r'_{2,}', '_', regex=True)
    s = s.replace(r'_+$', '', regex=True)
    return s


def transform_column_name(name: str) -> str:
    name = re.sub(r'[^\w\d]', '_', name)
    name = re.sub(r'(^\d+|^_)', r'col_\g<1>', name)
    name = re.sub(r'_{2,}', '_', name)
    name = re.sub(r'_+$', '', name)
    return name


def get_schema(s: pd.Series) -> pd.Series:
    """To get table schema from fields of Datamodel of Angora

    :param s: pd.Series
        [
            {"name": "\ucc28\uc885", "type": "TEXT", "alias": ""},
            {"name": "\uc0ac\uace0\uac74\uc218\ub300\ube44\uc0ac\ub9dd\uc790", "type": "REAL", "alias": ""}
        ]
    :return:
        차종 text, 사고건수대비사망자 real
    """
    s = s.apply(lambda x:
                ', '.join([
                    transform_column_name(f['name']) + ' ' + f['type'].lower()
                    for f in json.loads(x)]))
    return s


def make_query_pgsql(df: pd.DataFrame) -> pd.Series:
    base_query = 'DROP TABLE IF EXISTS {0}; CREATE TABLE {0} ({1});'
    return df[['table_name', 'schema']].apply(lambda x: base_query.format(x[0], x[1]), axis=1)


def run(args):
    df = pd.read_csv(args.input, encoding=args.encoding, sep=args.delimiter)
    df['table_name'] = get_table_name(df['NAME'])
    if len(df['table_name'].unique()) == df['table_name'].count():
        print('ok')
    if args.mode != 'query':
        df.to_csv('datamodel_w_table_name.csv',
                  sep=',',
                  header=True,
                  index=False,
                  quotechar='\"',
                  quoting=csv.QUOTE_MINIMAL,
                  encoding='utf-8')
    else:
        df['schema'] = get_schema(df['FIELDS'])
        df['sql'] = make_query_pgsql(df)

        df['sql'].to_csv('create_table.sql',
                         sep='\t',
                         header=False,
                         index=False,
                         quotechar='\"',
                         quoting=csv.QUOTE_MINIMAL,
                         encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Change dataset of Angora Metastore from origin data-source to new.")
    parser.add_argument('-i', '--input',
                        default='./datamodel.csv',
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
    parser.add_argument('-m', '--mode',
                        default='query',
                        choices=['query', 'table_name'],
                        type=str,
                        required=False,
                        help='Mode to select result format. (default=query)')

    print(parser.parse_args())

    run(parser.parse_args())

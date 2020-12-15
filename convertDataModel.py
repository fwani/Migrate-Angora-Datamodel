import argparse
import csv
import json
import pandas as pd


def transform_dataset(df: pd.DataFrame, args) -> pd.Series:
    base_str = '{"format": "%s", "table": "%s"}'
    return df[['DATASET', 'table_name']].apply(lambda x: base_str % (args.new, x[1]), axis=1)


def run(args):
    df = pd.read_csv(args.input, encoding=args.encoding, sep=args.delimiter)
    df['DATASET'] = transform_dataset(df, args)
    df = df.drop('table_name', axis=1)

    df.to_csv('datamodel_for_{}.csv'.format(args.new),
              sep=',',
              header=True,
              index=False,
              quotechar='\"',
              quoting=csv.QUOTE_MINIMAL,
              encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Change dataset of Angora Metastore from origin data-source to new.")
    parser.add_argument('-i', '--input',
                        default='./datamodel_w_table_name.csv',
                        type=str,
                        required=False,
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
    parser.add_argument('-O', '--old',
                        default='hdfs',
                        type=str,
                        required=False,
                        help='Insert old data-source.')
    parser.add_argument('-N', '--new',
                        default='postgresql',
                        type=str,
                        required=False,
                        help='Insert new data-source.')

    print(parser.parse_args())

    run(parser.parse_args())

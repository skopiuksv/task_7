import os
import pandas as pd
import logging
from config import *
from os.path import isfile, join
from time import sleep

filenum = 0
file_dict = {}


def file_checker(directory='./log_files'):
    global filenum
    global file_dict
    list_with_filenames = [f for f in os.listdir(directory) if (isfile(join(directory, f)) and f.endswith('.csv'))]
    if len(list_with_filenames) == 0:
        return None
    for file_name in list_with_filenames:
        if file_name in file_dict.values():
            continue
        else:
            file_dict[filenum] = file_name
            filenum += 1
            return file_name


def read_file(filename: str) -> pd.DataFrame:
    df = pd.read_csv(f'./log_files/{filename}')
    df.columns = columns
    df['date'] = pd.to_datetime(df['date'], unit='s')
    return df


def alert_result(df: pd.DataFrame, freq, *group_col) -> int:
    result = (
        df.query('severity == "Error"')
        .groupby([pd.Grouper(key='date', freq=freq, dropna=True), *group_col])['severity']
        .count()
    )
    res_amount = result[result > 10].count()
    return res_amount


def main():
    while True:
        file_name = file_checker()
        if file_name is None:
            logging.warning('no new files, retry in 10 seconds')
            sleep(10)
            continue
        else:
            logging.warning(f'new file {file_name} is found. processing...')
            df = read_file(file_name)  # reads file
            result_minutes = alert_result(df, '1Min')
            result_hours = alert_result(df, '60Min', 'bundle_id')
            print(f'File {file_name} is parsed. Found {result_minutes} errors less than in 1 minute')
            sleep(5)
            print(f'File {file_name} is parsed. Found {result_hours} errors less than in one hour for bundle_id')
            sleep(10)
            logging.warning(f'file {file_name} successfully uploaded, report is sent')
            continue

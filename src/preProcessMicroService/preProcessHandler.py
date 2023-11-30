import pandas as pd
from src import consts
import os
from src.dalteLakeMicorService.daltelLakeHandler import save_to_lake
from kafka import KafkaProducer


def round_data(df):
    columns_name = ['dist', 'metro_dist', 'realSum', 'attr_index_norm', 'rest_index_norm']
    for col in columns_name:
        df[col] = df[col].round(5)
    return df


def create_one_hot_encdoing(df):
    binary_columns = ['room_shared', 'room_private', 'host_is_superhost']
    for col in binary_columns:
        df[col] = df[col].map({True: 1, False: 0})
    df['room_type'] = df['room_type'].map({'Private room': 1, 'Entire home/apt': 0})
    return df


def create_new_table_with_id(df):
    df['id'] = df.groupby('dist').cumcount() + 1


def remove_columns(df):
    df['biz'] = (df['multi'] | df['biz'])
    names_of_columns = ["lat", "lng", "attr_index", "rest_index", "multi", "Unnamed: 0"]
    df.drop(columns=names_of_columns, inplace=True)
    return df


def drop_all_nan_and_dup(df):
    df.dropna(inplace=True)
    df.drop_duplicates(subset='dist', keep='first', inplace=True)
    return df


def sort_and_make_new_id(df):
    df.sort_values(by='dist', ascending=True, inplace=True)
    df["id"] = range(len(df))
    return df


def bin_data(df):
    columns_name = ["rest_index_norm", "attr_index_norm"]
    for name in columns_name:
        df[name] = pd.cut(df[name], bins=50, labels=range(50))
    return df


def pre_process(df):
    df = round_data(df)
    df = create_one_hot_encdoing(df)
    df = remove_columns(df)
    df = drop_all_nan_and_dup(df)
    df = sort_and_make_new_id(df)
    df = bin_data(df)
    return df


def start_pipline():
    dfs = {}
    files = os.listdir(consts.path_to_dataset)
    for file in files:
        if file != ".ipynb_checkpoints":
            df = pd.read_csv("airbnb_dataset\\" + file)
            df['person_capacity'] = df['person_capacity'].astype(float)
            save_to_lake(df, file)
            file = file.replace(".csv", "")
            dfs[file] = pre_process(df)
    return dfs




import pandas as pd
import os
import json
from kafka import KafkaProducer
from src import consts
from src.dalteLakeMicorService.daltelLakeHandler import save_to_lake


class PreProcessHandler:
    def __init__(self, kafka_server="localhost:9092", kafka_topic="processed_data"):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.kafka_topic = kafka_topic

    def send_to_kafka(self, data):
        self.kafka_producer.send(self.kafka_topic, data)
        self.kafka_producer.flush()

    def round_data(self, df):
        columns_name = ['dist', 'metro_dist', 'realSum', 'attr_index_norm', 'rest_index_norm']
        for col in columns_name:
            df[col] = df[col].round(5)
        return df

    def create_one_hot_encdoing(self, df):
        binary_columns = ['room_shared', 'room_private', 'host_is_superhost']
        for col in binary_columns:
            df[col] = df[col].map({True: 1, False: 0})
        df['room_type'] = df['room_type'].map({'Private room': 1, 'Entire home/apt': 0})
        return df

    def create_new_table_with_id(self, df):
        df['id'] = df.groupby('dist').cumcount() + 1

    def remove_columns(self, df):
        df['biz'] = (df['multi'] | df['biz'])
        names_of_columns = ["lat", "lng", "attr_index", "rest_index", "multi", "Unnamed: 0"]
        df.drop(columns=names_of_columns, inplace=True)
        return df

    def drop_all_nan_and_dup(self, df):
        df.dropna(inplace=True)
        df.drop_duplicates(subset='dist', keep='first', inplace=True)
        return df

    def sort_and_make_new_id(self, df):
        df.sort_values(by='dist', ascending=True, inplace=True)
        df["id"] = range(len(df))
        return df

    def bin_data(self, df):
        columns_name = ["rest_index_norm", "attr_index_norm"]
        for name in columns_name:
            df[name] = pd.cut(df[name], bins=50, labels=range(50))
        return df

    def pre_process(self, df):
        df = self.round_data(df)
        df = self.create_one_hot_encdoing(df)
        df = self.remove_columns(df)
        df = self.drop_all_nan_and_dup(df)
        df = self.sort_and_make_new_id(df)
        df = self.bin_data(df)
        return df

    def start_pipeline(self):
        files = os.listdir(consts.path_to_dataset)
        for file in files:
            if file != ".ipynb_checkpoints":
                df = pd.read_csv(os.path.join(consts.path_to_dataset, file))
                df['person_capacity'] = df['person_capacity'].astype(float)
                save_to_lake(df, file)  # Save original data to lake
                processed_df = self.pre_process(df)
                self.send_to_kafka(processed_df.to_json(orient='split'))  # Send processed data to Kafka

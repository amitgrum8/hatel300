import os
import json
import pandas as pd
from src import consts
from kafka import KafkaProducer


class PreProcessAndUploadToKafka:
    def __init__(self, kafka_server="localhost:9092", kafka_topic="processed_data_one"):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.kafka_topic = kafka_topic
        self.curr_df = None

    def round_data(self):
        columns_name = ['dist', 'metro_dist', 'realSum', 'attr_index_norm', 'rest_index_norm']
        for col in columns_name:
            self.curr_df[col] = self.curr_df[col].round(5)

    def create_one_hot_encdoing(self):
        binary_columns = ['room_shared', 'room_private', 'host_is_superhost']
        for col in binary_columns:
            self.curr_df[col] = self.curr_df[col].map({True: 1, False: 0})
        self.curr_df['room_type'] = self.curr_df['room_type'].map(
            {'Private room': 1, 'Entire home/apt': 0})

    def create_new_table_with_id(self):
        self.curr_df['id'] = self.curr_df.groupby('dist').cumcount() + 1

    def remove_columns(self):
        self.curr_df['biz'] = (self.curr_df['multi'] | self.curr_df['biz'])
        names_of_columns = ["lat", "lng", "attr_index", "rest_index", "multi", "Unnamed: 0"]
        self.curr_df.drop(columns=names_of_columns, inplace=True)
        self.curr_df['person_capacity'] = self.curr_df['person_capacity'].astype(float)

    def drop_all_nan_and_dup(self):
        self.curr_df.dropna(inplace=True)
        self.curr_df.drop_duplicates(subset='dist', keep='first', inplace=True)

    def sort_and_make_new_id(self):
        self.curr_df.sort_values(by='dist', ascending=True, inplace=True)
        self.curr_df["id"] = range(len(self.curr_df))

    def bin_data(self):
        columns_name = ["rest_index_norm", "attr_index_norm"]
        for name in columns_name:
            self.curr_df[name] = pd.cut(self.curr_df[name], bins=50, labels=False)

    def pre_process(self):
        self.round_data()
        self.create_one_hot_encdoing()
        self.remove_columns()
        self.drop_all_nan_and_dup()
        self.sort_and_make_new_id()
        self.bin_data()

    def start_pipeline(self):
        files = os.listdir(consts.PATH_TO_DATASET)
        for file_name in files:
            self.curr_df = pd.read_csv(os.path.join(consts.PATH_TO_DATASET, file_name))
            self.kafka_producer.send(os.getenv("DALTE_LAKE_ADD"), json.dumps(self.curr_df.to_dict(orient='split')))
            self.pre_process()
            file_name = file_name.replace(".csv", "")
            json_structure = {
                "df_data": self.curr_df.to_dict(orient="split"),  # DataFrame data
                "property_table_name": f"property_{file_name}",
                "renting_table_name": f"renting_{file_name}"
            }
            self.kafka_producer.send(self.kafka_topic, json_structure)
        self.kafka_producer.flush()

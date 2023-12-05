from src.KafkaMircoService.kafkaHandler import KafkaHandler  # Ensure this is implemented as per previous guidance
import pandas as pd
import os
from src import consts
from deltalake import DeltaTable
from deltalake.writer import write_deltalake


class DeltaLakeHandler:
    def __init__(self, kafka_handler: KafkaHandler):
        self.kafka_handler = kafka_handler

    def save_to_lake(self, data, table_name):
        write_deltalake(os.path.join(consts.path_to_datalake, table_name), data, mode="overwrite")

    def read_from_lake(self, file):
        dt = DeltaTable(os.path.join(consts.path_to_datalake, file))
        return dt.to_pandas()

    def consume_and_save(self, topic, table):
        self.kafka_handler.start_consumer(topic, group_id='delta_lake_group')
        for msg in self.kafka_handler.consume_messages():
            df = pd.read_json(msg.value, orient='split')
            self.save_to_lake(df, table)  #

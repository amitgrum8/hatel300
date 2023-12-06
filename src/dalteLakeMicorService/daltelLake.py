from src.KafkaMircoService.kafkaHandler import KafkaHandler
import pandas as pd
import os
import logging
from src import consts
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DeltaLakeHandler:
    def __init__(self, kafka_handler: KafkaHandler):
        self.kafka_handler = kafka_handler

    def save_to_lake(self, data, table_name):
        try:
            write_deltalake(os.path.join(consts.PATH_TO_DATALAKE, table_name), data, mode="overwrite")
            logger.info(f"Data successfully saved to {table_name} in Delta Lake.")
        except Exception as e:
            logger.error(f"Error saving data to Delta Lake: {e}")

    def read_from_lake(self, file):
        try:
            dt = DeltaTable(os.path.join(consts.PATH_TO_DATALAKE, file))
            return dt.to_pandas()
        except Exception as e:
            logger.error(f"Error reading from Delta Lake: {e}")
            return None

    def consume_and_save(self, topic, table):
        try:
            self.kafka_handler.start_consumer(topic, group_id='delta_lake_group')
            for msg in self.kafka_handler.consume_messages():
                if isinstance(msg.value, str):
                    try:
                        df = pd.read_json(msg.value, orient='split')
                        self.save_to_lake(df, table)
                    except ValueError as e:
                        logger.error(f"Error processing message from Kafka: {e}")
                else:
                    logger.warning(f"Received non-string message: {msg.value}")
        except Exception as e:
            logger.error(f"Error in consume_and_save method: {e}")

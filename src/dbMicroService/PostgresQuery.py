import json
import logging
import pandas as pd

from src import consts
from src.KafkaMircoService.kafkaHandler import KafkaHandler
from sqlalchemy import create_engine, inspect

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresQueryService:
    def __init__(self, kafka_handler: KafkaHandler, db_config):
        logger.info("Initializing PostgreSQL service")
        self.kafka_handler = kafka_handler
        self.engine = create_engine(
            f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')

    def load_table_as_df(self, table_name):
        try:
            df = pd.read_sql_table(table_name, self.engine)
            logger.info(f"Data loaded successfully from table {table_name}")
            return df
        except Exception as e:
            logger.error(f"Error loading data from table {table_name}: {e}")
            return None

    def produce_all_tables_to_kafka(self, topic_name=consts.DS_DATA_TOPIC):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        for table_name in table_names:
            df = self.load_table_as_df(table_name)
            if df is not None:
                try:
                    # Create a JSON object with both the table name and the data
                    json_data = {
                        'table_name': table_name,
                        'data': df.to_json(orient='records', date_format='iso')
                    }
                    json_payload = json.dumps(json_data)  # Convert the object to a JSON string

                    # Send the JSON payload to Kafka
                    self.kafka_handler.send_message(topic_name, json_payload)
                    logger.info(f"Data from table {table_name} sent to Kafka topic {topic_name}")
                except Exception as e:
                    logger.error(f"Error sending data to Kafka for table {table_name}: {e}")




# Usage example
kafka_handler = KafkaHandler()  # Ensure KafkaHandler is implemented properly
postgres_service = PostgresQueryService(kafka_handler, consts.db_config)
postgres_service.produce_all_tables_to_kafka()

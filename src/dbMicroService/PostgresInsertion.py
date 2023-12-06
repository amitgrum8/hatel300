import logging
import pandas as pd
from src import consts
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from src.dbMicroService.models.propertyData import create_property_data_class
from src.dbMicroService.models.rentingData import create_renting_data_class
from src.dbMicroService.models.mainTable import MainTable, Base as MainBase
from src.KafkaMircoService.kafkaHandler import KafkaHandler
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine
import ast

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresInsertionService:
    def __init__(self, kafka_handler: KafkaHandler, db_config):
        logger.info("Initializing PostgreSQL service")
        self.kafka_handler = kafka_handler
        self.engine = create_engine(
            f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
        MainBase.metadata.create_all(self.engine)

    def create_relationships(self, property_class, renting_class):
        renting_class.property = relationship(property_class, back_populates="renting_data")
        property_class.renting_data = relationship(renting_class, back_populates="property")

    def compute_average_price(self, table_name):
        if 'renting' in table_name:
            with self.engine.connect() as conn:
                result = conn.execute(text(f'SELECT AVG("realSum") FROM {table_name}'))
                return result.scalar()
        else:
            return None

    def update_main_table(self, table_name, session):
        with self.engine.connect() as conn:
            record_count_query = text(f"SELECT COUNT(*) FROM {table_name}")
            record_count = conn.execute(record_count_query).scalar()
            average_price = self.compute_average_price(table_name)

            existing_record = session.query(MainTable).filter_by(table_name=table_name).first()
            if existing_record:
                existing_record.record_count = record_count
                existing_record.average_price = average_price
                session.merge(existing_record)
            else:
                new_record = MainTable(table_name=table_name, record_count=record_count, average_price=average_price)
                session.add(new_record)

            session.commit()
            session.close()

    def create_all_tables(self, property_table_name, renting_table_name):
        logger.info("Creating all tables")
        PropertyClass = create_property_data_class(property_table_name)
        RentingClass = create_renting_data_class(renting_table_name, property_table_name)
        self.create_relationships(PropertyClass, RentingClass)
        MainBase.metadata.create_all(self.engine)

    def insert_data(self, df, property_table_name, renting_table_name):
        PropertyClass = create_property_data_class(property_table_name)
        RentingClass = create_renting_data_class(renting_table_name, property_table_name)
        self.create_relationships(PropertyClass, RentingClass)
        property_columns = ast.literal_eval(consts.PROPERTY_COLUMNS)
        renting_columns = ast.literal_eval(consts.RENTING_COLUMNS)
        df_property = df[property_columns]
        df_renting = df[renting_columns]

        with sessionmaker(bind=self.engine)() as session:
            try:
                for index, row in df_property.iterrows():
                    session.add(PropertyClass(**row.to_dict()))
                for index, row in df_renting.iterrows():
                    session.add(RentingClass(**row.to_dict(), property_id=row['id']))
                session.commit()
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"An error occurred during data insertion: {e}")
            self.update_main_table(property_table_name, session)
            self.update_main_table(renting_table_name, session)
            session.close()

    def consume_and_insert(self, topic):
        logger.info(f"Starting to consume from Kafka topic: {topic}")
        try:
            self.kafka_handler.start_consumer(topic, group_id='postgres_group')
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")
            return

        for msg in self.kafka_handler.consume_messages():
            if msg is None:
                continue
            logger.info("Received a message from Kafka")

            json_data = msg.value
            df_data = json_data['df_data']
            df = pd.DataFrame(df_data['data'], columns=df_data['columns'])

            property_table_name = json_data['property_table_name']
            renting_table_name = json_data['renting_table_name']

            self.create_all_tables(property_table_name, renting_table_name)
            self.insert_data(df, property_table_name, renting_table_name)
            logger.info(f"Data inserted into {property_table_name} and {renting_table_name}")


# Configuration for PostgreSQL
db_config = {
    "host": consts.POSTGRES_SERVER,
    "port": consts.POSTGRES_PORT,
    "database": consts.DATABASE_NAME,
    "user": consts.POSTGRES_USERNAME,
    "password": consts.POSTGRES_PASSWORD
}

kafka_handler = KafkaHandler()  # Ensure KafkaHandler is implemented properly
postgres_service = PostgresInsertionService(kafka_handler, db_config)
postgres_service.consume_and_insert("processed_data_one")

import logging
import pandas as pd
from src.KafkaMircoService.kafkaHandler import KafkaHandler
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from sqlalchemy.sql import text
from src import consts
import json

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresInsertionService:
    Base = declarative_base()

    class DynamicBase(Base):
        __abstract__ = True
        id = Column(Integer, primary_key=True)

    class MainTable(Base):
        __tablename__ = 'main_table'
        id = Column(Integer, primary_key=True)
        table_name = Column(String, unique=True)
        record_count = Column(Integer)
        average_price = Column(Float)

    def __init__(self, kafka_handler: KafkaHandler, db_config):
        logger.info("Initializing PostgreSQL service")
        self.kafka_handler = kafka_handler
        self.engine = create_engine(
            f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
        self.Base.metadata.create_all(self.engine)

    def create_property_class(self, table_name):
        class PropertyData(self.DynamicBase):
            __tablename__ = table_name
            __table_args__ = {'extend_existing': True}
            id = Column(Integer, primary_key=True)
            rest_index_norm = Column(Float)
            attr_index_norm = Column(Float)
            room_type = Column(String)
            room_shared = Column(Integer)
            room_private = Column(Integer)
            person_capacity = Column(Integer)
            bedrooms = Column(Integer)
            dist = Column(Float)
            metro_dist = Column(Float)

        return PropertyData

    def create_renting_class(self, table_name, property_table_name):
        class RentingData(self.DynamicBase):
            __tablename__ = table_name
            __table_args__ = {'extend_existing': True}
            id = Column(Integer, primary_key=True)
            property_id = Column(Integer, ForeignKey(f'{property_table_name}.id'))
            realSum = Column(Float)
            biz = Column(String)
            host_is_superhost = Column(String)
            guest_satisfaction_overall = Column(Float)
            cleanliness_rating = Column(Float)

        return RentingData

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

    def update_main_table(self, table_name):
        with self.engine.connect() as conn:
            record_count_query = text(f"SELECT COUNT(*) FROM {table_name}")
            record_count = conn.execute(record_count_query).scalar()
            average_price = self.compute_average_price(table_name)

            Session = sessionmaker(bind=self.engine)
            session = Session()

            existing_record = session.query(self.MainTable).filter_by(table_name=table_name).first()
            if existing_record:
                existing_record.record_count = record_count
                existing_record.average_price = average_price
                session.merge(existing_record)
            else:
                new_record = self.MainTable(table_name=table_name, record_count=record_count,
                                            average_price=average_price)
                session.add(new_record)

            session.commit()
            session.close()

    def create_all_tables(self, property_table_name, renting_table_name):
        logger.info("Creating all tables")
        PropertyClass = self.create_property_class(property_table_name)
        RentingClass = self.create_renting_class(renting_table_name, property_table_name)
        self.create_relationships(PropertyClass, RentingClass)
        self.Base.metadata.create_all(self.engine)

        self.update_main_table(property_table_name)
        self.update_main_table(renting_table_name)

    def insert_data(self, df, property_table_name, renting_table_name):
        PropertyData = self.create_property_class(property_table_name)
        RentingData = self.create_renting_class(renting_table_name, property_table_name)
        self.create_relationships(PropertyData, RentingData)

        df_property = df[
            ['id', 'rest_index_norm', 'attr_index_norm', 'room_type', 'room_shared', 'room_private', 'person_capacity',
             'bedrooms', 'dist', 'metro_dist']]
        df_renting = df[
            ['id', 'realSum', 'biz', 'host_is_superhost', 'guest_satisfaction_overall', 'cleanliness_rating']]

        Session = sessionmaker(bind=self.engine)
        session = Session()

        try:
            for index, row in df_property.iterrows():
                session.add(PropertyData(**row.to_dict()))

            for index, row in df_renting.iterrows():
                session.add(RentingData(**row.to_dict(), property_id=row['id']))
            session.commit()
            self.update_main_table(property_table_name)
            self.update_main_table(renting_table_name)
        except Exception as e:
            session.rollback()
            print(f"An error occurred during data insertion: {e}")
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

            # Directly use the message value assuming it's already a Python dictionary
            json_data = msg.value  # Remove json.loads() as msg.value is already a dictionary

            df_data = json_data['df_data']
            df = pd.DataFrame(df_data['data'], columns=df_data['columns'])

            property_table_name = json_data['property_table_name']
            renting_table_name = json_data['renting_table_name']

            # Create tables if they don't exist
            self.create_all_tables(property_table_name, renting_table_name)

            # Insert data into tables
            self.insert_data(df, property_table_name, renting_table_name)
            logger.info(f"Data inserted into {property_table_name} and {renting_table_name}")


db_config = {
    "host": consts.postgres_server,
    "port": consts.postgres_port,
    "database": consts.database_name,
    "user": consts.postgres_username,
    "password": consts.postgres_password
}

kafka_handler = KafkaHandler()  # Ensure KafkaHandler is implemented properly
postgres_service = PostgresInsertionService(kafka_handler, db_config)
postgres_service.consume_and_insert("processed_data_one")

import pandas as pd
import logging
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from src import consts
from src.main import get_all_dfs

# Enable logging for SQL statements
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Database configuration
db_config = {
    "host": consts.postgres_server,
    "port": consts.postgres_port,
    "database": consts.database_name,
    "user": consts.postgres_username,
    "password": consts.postgres_password
}

# Establishing the connection using psycopg2 and creating an engine
try:
    conn = psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"]
    )

    engine = create_engine(
        f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
    conn.close()  # Close the psycopg2 connection
    print("Connection to the database established.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

Base = declarative_base()


class DynamicBase(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)


def create_property_class(table_name):
    class PropertyData(DynamicBase):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        # Relationship will be added later
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


def create_renting_class(table_name, property_table_name):
    class RentingData(DynamicBase):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        property_id = Column(Integer, ForeignKey(f'{property_table_name}.id'))
        # Relationship will be added later
        realSum = Column(Float)
        biz = Column(String)
        host_is_superhost = Column(String)
        guest_satisfaction_overall = Column(Float)
        cleanliness_rating = Column(Float)

    return RentingData


def create_relationships(property_class, renting_class):
    property_class.renting_data = relationship(renting_class, back_populates="property")
    renting_class.property = relationship(property_class, back_populates="renting_data")


def create_all_tables(dfs):
    for name in dfs.keys():
        PropertyClass = create_property_class(f'property_{name}')
        RentingClass = create_renting_class(f'renting_{name}', f'property_{name}')
        create_relationships(PropertyClass, RentingClass)
        Base.metadata.create_all(engine)


def insert_data(df, property_table_name, renting_table_name):
    PropertyData = create_property_class(property_table_name)
    RentingData = create_renting_class(renting_table_name, property_table_name)
    create_relationships(PropertyData, RentingData)

    df_property = df[['id', 'rest_index_norm', 'attr_index_norm', 'room_type', 'room_shared', 'room_private', 'person_capacity', 'bedrooms', 'dist', 'metro_dist']]
    df_renting = df[['id', 'realSum', 'biz', 'host_is_superhost', 'guest_satisfaction_overall', 'cleanliness_rating']]

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for index, row in df_property.iterrows():
            session.add(PropertyData(**row.to_dict()))

        for index, row in df_renting.iterrows():
            session.add(RentingData(**row.to_dict(), property_id=row['id']))

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"An error occurred during data insertion: {e}")


# Main execution
try:
    dfs = get_all_dfs()
    create_all_tables(dfs)  # Create all tables before inserting data
    for name, df in dfs.items():
        insert_data(df, f'property_{name}', f'renting_{name}')
except Exception as e:
    print(f"An error occurred: {e}")

# Reminder to check PostgreSQL logs if issues arise
print("If any issues arise, please check the PostgreSQL server logs for more details.")

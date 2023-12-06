import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

env_vars = [
    'PATH_TO_DATASET',
    'PATH_TO_DATALAKE',
    'POSTGRES_SERVER',
    'DATABASE_NAME',
    'POSTGRES_PORT',
    'POSTGRES_USERNAME',
    'POSTGRES_PASSWORD',
    'DS_DATA_TOPIC',
    'PROPERTY_COLUMNS',
    "RENTING_COLUMNS"
]


def check_env_variables(env_vars):
    missing_vars = [var for var in env_vars if not os.getenv(var)]
    if missing_vars:
        error_message = f"Missing environment variables: {', '.join(missing_vars)}"
        logger.error(error_message)
        raise EnvironmentError(error_message)


check_env_variables(env_vars)

PATH_TO_DATASET = os.getenv('PATH_TO_DATASET')
PATH_TO_DATALAKE = os.getenv('PATH_TO_DATALAKE')
POSTGRES_SERVER = os.getenv('POSTGRES_SERVER')
DATABASE_NAME = os.getenv('DATABASE_NAME')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DS_DATA_TOPIC = os.getenv("DS_DATA_TOPIC")
PROPERTY_COLUMNS = os.getenv("PROPERTY_COLUMNS")
RENTING_COLUMNS = os.getenv("RENTING_COLUMNS")



db_config = {
    "host": POSTGRES_SERVER,
    "port": POSTGRES_PORT,
    "database": DATABASE_NAME,
    "user": POSTGRES_USERNAME,
    "password": POSTGRES_PASSWORD
}

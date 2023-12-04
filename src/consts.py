from dotenv import load_dotenv
import os

load_dotenv()
path_to_dataset = os.getenv('PATH_TO_DATASET')
path_to_datalake=os.getenv('PATH_TO_DATALAKE')
postgres_server = os.getenv('POSTGRES_SERVER')
database_name = os.getenv('DATABASE')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_username=os.getenv('POSTGRES_USER_NAME')
postgres_password = os.getenv('POSTGRES_PASSWORD')

db_config = {
    "host": postgres_server,
    "port": postgres_port,
    "database": database_name,
    "user": postgres_username,
    "password": postgres_password
}
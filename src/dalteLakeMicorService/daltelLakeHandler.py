from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import os
from src import consts


def save_to_lake(data,table_name):
    write_deltalake(os.path.join(consts.path_to_datalake,table_name), data, mode="overwrite" or "append")


def read_from_lake(file):
    dt = DeltaTable(os.path.join(consts.path_to_datalake,file))
    df = dt.to_pandas()
    return df

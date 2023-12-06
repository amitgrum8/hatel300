from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

# Shared metadata
shared_metadata = MetaData()

# Single declarative base using the shared metadata
Base = declarative_base(metadata=shared_metadata)

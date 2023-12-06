from sqlalchemy import Column, Integer
from src.dbMicroService.models.base import Base


class DynamicBase(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)

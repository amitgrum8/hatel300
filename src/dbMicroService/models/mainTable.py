from sqlalchemy import Column, Integer, Float, String
from src.dbMicroService.models.base import Base



class MainTable(Base):
    __tablename__ = 'main_table'
    id = Column(Integer, primary_key=True)
    table_name = Column(String, unique=True)
    record_count = Column(Integer)
    average_price = Column(Float)
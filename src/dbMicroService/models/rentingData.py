from sqlalchemy import Column, Integer, Float, String, ForeignKey
from src.dbMicroService.models.dynamicBase import DynamicBase

def create_renting_data_class(table_name, property_table_name):
    class RentingData(DynamicBase):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}
        property_id = Column(Integer, ForeignKey(f'{property_table_name}.id'))
        realSum = Column(Float)
        biz = Column(String)
        host_is_superhost = Column(String)
        guest_satisfaction_overall = Column(Float)
        cleanliness_rating = Column(Float)

    return RentingData

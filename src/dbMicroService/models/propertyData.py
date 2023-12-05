from sqlalchemy import Column, Integer, Float, String
from dynamicBase import DynamicBase

def create_property_data_class(table_name):
    class PropertyData(DynamicBase):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}
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

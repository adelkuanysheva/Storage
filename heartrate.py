from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class HeartRate(Base):
    """ Heart Rate """

    __tablename__ = "heart_rate"

    ride_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(250), nullable=False)
    device_id = Column(String(250), nullable=False)
    heart_rate = Column(Integer, nullable=False)
    max_hr = Column(Integer, nullable=False)
    min_hr = Column(Integer, nullable=False)
    timestamp = Column(String(100), nullable=False)
    traceID = Column(String(255), nullable=False)
    date_created = Column(DateTime, nullable=True)



    def __init__(self, user_id, device_id, heart_rate, max_hr, min_hr, timestamp, traceID):
        """ Initializes a heart rate reading """
        self.user_id = user_id
        self.device_id = device_id
        self.heart_rate = heart_rate
        self.max_hr = max_hr
        self.min_hr = min_hr
        self.timestamp = timestamp
        self.traceID = traceID
        self.date_created = datetime.datetime.now() # Sets the date/time record is created


    def to_dict(self):
        """ Dictionary Representation of a heart rate reading """
        dict = {}
        dict['ride_id'] = self.ride_id
        dict['user_id'] = self.user_id
        dict['device_id'] = self.device_id
        dict['heart_rate'] = self.heart_rate
        dict['max_hr'] = self.max_hr
        dict['min_hr'] = self.min_hr
        dict['timestamp'] = self.timestamp
        dict['traceID'] = self.traceID
        dict['date_created'] = self.date_created


        return dict

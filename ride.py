from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Ride(Base):
    """ Blood Pressure """

    __tablename__ = "ride"

    ride_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(250), nullable=False)
    movie = Column(String(250), nullable=False)
    timestamp = Column(String(100), nullable=False)
    avg_speed = Column(Integer, nullable=False)
    avg_power = Column(Integer, nullable=False)
    distance = Column(Integer, nullable=False)
    traceID = Column(String(255), nullable=False)
    date_created = Column(DateTime, nullable=False)
    

    def __init__(self, user_id, movie, timestamp, avg_speed, avg_power, distance, traceID):
        self.user_id = user_id
        self.movie = movie
        self.timestamp = timestamp
        self.avg_speed = avg_speed
        self.avg_power = avg_power
        self.distance = distance
        self.traceID = traceID
        self.date_created = datetime.datetime.now() # Sets the date/time record is created

    def to_dict(self):
        """ Dictionary Representation of a blood pressure reading """
        dict = {}
        dict['ride_id'] = self.ride_id
        dict['user_id'] = self.user_id
        dict['movie'] = self.movie
        dict['timestamp'] = self.timestamp
        dict['avg_speed'] = self.avg_speed
        dict['avg_power'] = self.avg_power
        dict['distance'] = self.distance
        dict['traceID'] = self.traceID
        dict['date_created'] = self.date_created

        return dict

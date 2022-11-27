import swagger_ui_bundle
import connexion
from connexion import NoContent
import json
import datetime 
import requests
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from base import Base
from ride import Ride
from heartrate import HeartRate
import mysql.connector
import pymysql 
import yaml
import logging
import logger
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import os 


if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

logger = logging.getLogger('basicLogger')

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
    
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)


MAX_EVENTS = 10
DB_ENGINE = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'
        .format(app_config['datastore']["user"], 
                app_config['datastore']["password"], 
                app_config['datastore']["hostname"], 
                app_config['datastore']["port"],
                app_config['datastore']["db"]))
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def storeRide(payload):

    trace_id = payload['traceID']
    session = DB_SESSION()
    bp = Ride(payload['user_id'], 
                    payload['movie'],
                    payload['timestamp'],
                    payload['avg_speed'],
                    payload['avg_power'],
                    payload['distance'], 
                    payload['traceID'])
    session.add(bp)
    session.commit()
    session.close()
    logger.info(f'Stored event rideEvent with a trace id of {trace_id}')
    
    logger.info('Connecting to DB. Hostname: {}, Port: {}'.format(app_config['datastore']["hostname"], app_config['datastore']["port"]))
    return "Saved ride reading to db"


def get_ride(start_timestamp, end_timestamp):
    """ Gets new ride readings after the timestamp """
    
    session = DB_SESSION()
    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(Ride).filter(
                        and_(Ride.date_created >= start_timestamp_datetime, 
                            Ride.date_created < end_timestamp_datetime))
    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for ride data readings after %s returns %d results" % (start_timestamp, len(results_list)))
    return results_list, 200

    
def storeHeartrate(payload):
    trace_id = payload['traceID']
    session = DB_SESSION()
    bp = HeartRate(payload['user_id'],
                    payload['device_id'],
                    payload['heart_rate'],
                    payload['max_hr'],
                    payload['min_hr'],
                    payload['timestamp'],
                    payload['traceID'])
    session.add(bp)
    session.commit()
    session.close()
    logger.info(f'Stored event heartrateEvent with a trace id of {trace_id}')
    
    logger.info('Connecting to DB. Hostname: {}, Port: {}'.format(app_config['datastore']["hostname"], app_config['datastore']["port"]))
    return "Saved ride reading to db"


def get_heartrate(start_timestamp, end_timestamp):
    """ Gets new heartrate readings after the timestamp """
    session = DB_SESSION()
    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    readings = session.query(HeartRate).filter(
                        and_(HeartRate.date_created >= start_timestamp_datetime, 
                            HeartRate.date_created < end_timestamp_datetime))
    results_list = []

    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for heartrate data readings after %s returns %d results" % (start_timestamp, len(results_list)))
    return results_list, 200


def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (app_config["events"]["hostname"], app_config["events"]["port"])

    curr_retry = 0
    while curr_retry < app_config["events"]["max_retry"]:
        try:
            logger.info(f"Trying to connect to Kafka, retry count: {curr_retry}")
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])]
        except:
            logger.error("Connection Failed!")
            time.sleep(app_config["events"]["sleep"])
        curr_retry += 1


    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).

    consumer = topic.get_simple_consumer(consumer_group=b'event_group', 
                                        reset_offset_on_start=False, 
                                        auto_offset_reset=OffsetType.LATEST)

    # This is blocking - it will wait for a new message
    for msg in consumer:
        print(msg)
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]

        if msg["type"] == "ride": # Change this to your event type
        # Store the event1 (i.e., the payload) to the DB
            storeRide(payload)
        elif msg["type"] == "heartrate": # Change this to your event type
            storeHeartrate(payload)
            # Store the event2 (i.e., the payload) to the DB
            # Commit the new message as being read
        consumer.commit_offsets()


def health():
    logger.info("Health Check returned 200")
    return 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
    #blah
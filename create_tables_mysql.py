import mysql.connector
import yaml

with open('./app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conn = mysql.connector.connect(host=app_config["datastore"]["hostname"], 
                                    user=app_config["datastore"]["user"],     
                                password=app_config["datastore"]["password"], 
                                database=app_config["datastore"]["db"])

db_cursor = db_conn.cursor()

db_cursor.execute('''
        CREATE TABLE ride
        (ride_id INT NOT NULL AUTO_INCREMENT,
        user_id VARCHAR(250) NOT NULL,
        movie VARCHAR(250) NOT NULL,
        timestamp VARCHAR(250) NOT NULL,
        avg_speed INTEGER NOT NULL,
        avg_power INTEGER NOT NULL,
        distance INTEGER NOT NULL,
        date_created VARCHAR(100) NOT NULL,
        traceID VARCHAR(255) NOT NULL,
        CONSTRAINT ride_id_pk PRIMARY KEY (ride_id))
        ''')

db_cursor.execute('''
        CREATE TABLE heart_rate
        (ride_id INT NOT NULL AUTO_INCREMENT,
        user_id VARCHAR(250) NOT NULL,
        device_id VARCHAR(250) NOT NULL,
        heart_rate INTEGER NOT NULL,
        max_hr INTEGER NOT NULL,
        min_hr INTEGER NOT NULL,
        timestamp VARCHAR(100) NOT NULL,
        date_created VARCHAR(100) NOT NULL,
        traceID VARCHAR(255) NOT NULL,
        CONSTRAINT heart_rate_pk PRIMARY KEY (ride_id))
        ''')

db_conn.commit()
db_conn.close()
import sqlite3

conn = sqlite3.connect('readings.sqlite')

c = conn.cursor()
c.execute('''
          CREATE TABLE ride
          (ride_id INTEGER PRIMARY KEY ASC, 
           user_id VARCHAR(250) NOT NULL,
           movie VARCHAR(250) NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
            avg_speed INTEGER NOT NULL,
           avg_power INTEGER NOT NULL,
           distance INTEGER NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           traceID VARCHAR(255) NOT NULL)
          ''')

c.execute('''
          CREATE TABLE heart_rate
          (ride_id INTEGER PRIMARY KEY ASC, 
           user_id VARCHAR(250) NOT NULL,
           device_id VARCHAR(250) NOT NULL,
           heart_rate INTEGER NOT NULL,
           max_hr INTEGER NOT NULL,
           min_hr INTEGER NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           traceID VARCHAR(255) NOT NULL)
          ''')

conn.commit()
conn.close()
from os import environ
from time import sleep
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, text
from sqlalchemy.exc import OperationalError
from jinja2 import Template

print('Waiting for the data generator...')
sleep(50)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')
# Write the solution here

extract_sql = Template("SELECT device_id, EXTRACT(DAY FROM TO_TIMESTAMP(CAST(time AS int))) AS day, EXTRACT(HOUR FROM TO_TIMESTAMP(CAST(time AS int))) AS hour, MAX(temperature) AS maximum_temperature, COUNT(1) AS total_data_points, SUM(ACOS(SIN(latitude) * SIN(previous_latitude) + COS(latitude) * COS(previous_latitude) * COS(previous_longitude - longitude)) * 6371) AS total_distance FROM (SELECT device_id, temperature, time, loc.latitude, loc.longitude, LAG(loc.latitude) OVER (PARTITION BY device_id ORDER BY time) AS previous_latitude, LAG(loc.longitude) OVER (PARTITION BY device_id ORDER BY time) AS previous_longitude FROM devices, JSON_TO_RECORD(CAST(location AS json)) AS loc(latitude FLOAT, longitude FLOAT) WHERE \"time\" >= '0') a GROUP BY 1, 2, 3;")
load_table_ddl = "CREATE TABLE device_fact_hourly (device_id VARCHAR(255),day INT,hour INT,maximum_temperature INT,total_data_points INT, total_distance float);"

def initialize_db():
    connection = None
    try:
        load_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, isolation_level='AUTOCOMMIT')
        ## Add destination table.
        connection = load_engine.connect()
        connection.execute(text(load_table_ddl))
        result = connection.execute(text('select * from device_fact_hourly;'))
        print(result.fetchall())
        print("Load table created.")
    except OperationalError as e:
        print("Database initialize error", e)
    finally:
        if connection is not None:
            connection.close()
def get_max_timestamp():
    try:
        file = open("timestamp.txt", "r")
        return str(file.read())
    except Exception as e:
        print(e)
    finally:
        file.close()

def put_max_timestamp(timestamp):
    try:
        file = open("timestamp.txt", "w")
        file.write(timestamp)
    except exception as e:
        print(e.message)
    finally:
        file.close()
    
def extract():
    connection = None
    try:
        extract_engine = create_engine(environ["POSTGRESQL_CS"])
        connection = extract_engine.connect()
        max_unix_timestamp = get_max_timestamp()
        result = connection.execute(text(extract_sql.render(max_unix_timestamp=max_unix_timestamp)))
        return result.mappings().all()
    except Exception as e:
        print(e)
    finally:
        connection.close()
        
def load(data):
    connection = None
    try:
        metadata = MetaData()
        load_table = Table(
        'device_fact_hourly', metadata,
        Column('device_id', String(255)),
        Column('day', Integer),
        Column('hour', Integer),
        Column('maximum_temperature', Integer),
        Column('total_data_points', Integer),
        Column('total_distance', Float)
        )
        for row in data:
            insert = load_table.insert().values(
                device_id=row["device_id"],
                day=row["day"],
                hour=row["hour"],
                maximum_temperature=row["maximum_temperature"],
                total_data_points=row["total_data_points"],
                total_distance=row["total_distance"]
            )
        load_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, isolation_level='AUTOCOMMIT')
        connection = load_engine.connect()
        connection.execute(insert)
        print("Load data complete.")
    except OperationalError as e:
        print(e)
    pass
# if __name__ = "__main__":
#     extract()
initialize_db()
data = extract()
load(data)

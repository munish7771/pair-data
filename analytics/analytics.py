from os import environ
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

global extract_engine, load_engine
load_table_ddl = "CREATE TABLE device_fact_hourly (device_id VARCHAR(255),day INT,hour INT,max_temperature INT,data_points INT,total_movement INT);"

def initialize():
    try:
        extract_engine = create_engine(environ["POSTGRESQL_CS"])
        load_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, isolation_level='AUTOCOMMIT')
        ## Add destination tables
        with load_engine.connect() as connection:
            connection.execute(text(load_table_ddl))
            result = connection.execute(text('select count(1) from device_fact_hourly;'))
            print(result.fetchall())
            print("Load table created.")
    except OperationalError as e:
        print("Database initialize error", e.message)
    finally:
        if connection is not None:
            connection.close()

initialize()

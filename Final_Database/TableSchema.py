import psycopg2
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import username, password

#CREATE DATABASE
create_db_query = 'CREATE DATABASE MovieDB'
create_schema_query = 'CREATE SCHEMA IF NOT EXISTS ETL'

# CREATE TABLE SCHEMA
ETL_actors = '''DROP TABLE IF EXISTS actors_role;
                CREATE TABLE actors_role(id INT PRIMARY KEY
                                   ,names VARCHAR
                                   ,last_role VARCHAR)'''
ETL_movies_info = '''DROP TABLE IF EXISTS movies_info;
                     CREATE TABLE movies_info(movie_id INT PRIMARY KEY
                                             ,title VARCHAR
                                             ,plot  VARCHAR
                                             ,IMDb_rating NUMERIC
                                             ,main_cast_id INT
                                             ,main_cast_name VARCHAR
                                             ,other_cast_id INT
                                             ,other_cast_name VARCHAR
                                             )'''


#CONNECT TO DATABASE AND CREATE CURSOR OBJECT TO DO THINGS
conn = psycopg2.connect(database="postgres", user=username, password=password, host='localhost', port= '5432')
conn.autocommit = True    #or we could commit with conn.commit()
cursor = conn.cursor()

#CREATE DATABASE
cursor.execute(create_db_query)
print("Successfully created database")
cursor.execute(ETL_actors)
print("Successfully created actors_role")
cursor.execute(ETL_movies_info)
print("Successfully created movies_info")

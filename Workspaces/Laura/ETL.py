#IMPORT DEPENDENCIES
import psycopg2
import pandas as pd
from config import username, password

#TABLE NAMES
actors = 'actors_role'
movies = 'movies_info'
host = 'localhost'
port='5432'

#DEFINE FUNCTION(S)
def load_df(df, table):
    df.to_sql(table, engine)

def load_csv(csv_path, table):
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            cursor.execute(
            "INSERT INTO {table} VALUES (%s, %s, %s)",
            row
            )

#START UP PostgreSQL
postgres_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(postgres_string)
conn = psycopg2.connect(database="postgres", user=username, password=password, host='localhost', port= '5432')
conn.autocommit = True    #or we could commit with conn.commit()
cursor = conn.cursor()

#LOAD DATA TO PostgreSQL
#Load Pandas DF Using SQLAlchemy
# Movies Table
df = movie_df
table = actors
load_df(df,table)

# Actors Table
df = cast_df
table = movies
load_df(df,table)
#Load CSVs
#csv_name = 'actor_dummy.csv'
#table = "actors_role"
#load_table(csv_path, table)

#CLOSE CONNECTION
conn.close()
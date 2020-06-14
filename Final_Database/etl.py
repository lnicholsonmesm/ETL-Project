#Import dependencies
from bs4 import BeautifulSoup
import requests
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from datetime import date, time, datetime, timedelta
import time

#get some time parameters
d = date.today()
start = datetime.now()
print(f"Code start time is: {start}")

#Grab response from Rotten Tomatoes URL
url = "https://editorial.rottentomatoes.com/guide/best-black-movies-21st-century/"
response = requests.get(url)
print(response.text)

#Create soup variable
soup = BeautifulSoup(response.content, 'html.parser')

#Search results in Header 2
results = soup.find_all("h2")
results
#Get soup movie names
movies = []

for result in results:
    title = result.text
    movies.append(title)
    print(title)
movies = movies[0:100]

#Replacing Fast Color 2019 because IMDb has it as released in 2018
movies[2] = movies[2].replace("(2019)", "(2018)")

#create instance of IMDb 
from imdb import IMDb
ia = IMDb()


#Set sleep timer to not overwhelm IMDB API
now = datetime.now()

#Loop through Rotten Tomatoes 100 Best Black Movies of the 21st Century to get their movie name on IMDb
movie_names=[]
for movie in movies:
    start_time = datetime.now()
    movie_results = ia.search_movie(movie)
    movie_names.append(movie_results[0]["title"])
    print(movie_results)
    time.sleep(1)
time_elapsed = datetime.now() - now
print(time_elapsed)
print(movie_names)

#Using their IMDb name, grab these movies ids, ratings, and cast names
movie_ids = []

for movie_name in movie_names:
    time.sleep(2)   
    movie_id = ia.search_movie(movie_name)[0].getID()
    movie_ids.append(movie_id)

#Using their IMDb ID, grab their IMDb rating and first two cast members
movie_ratings=[]
cast_0_names = []
cast_1_names = []

for movie_id in movie_ids:
    time.sleep(2)
    try:
        rating = ia.get_movie(movie_id).get("rating")
        movie_ratings.append(rating)
        print(rating)
    except:
        movie_ratings.append(None)
    try:
        cast_0_name = ia.get_movie(movie_id).get("cast")[0]["name"]
        cast_1_name = ia.get_movie(movie_id).get("cast")[1]["name"]
        cast_0_names.append(cast_0_name)
        cast_1_names.append(cast_1_name)
    except:
        cast_0_names.append(None)
        cast_1_names.append(None)
    
#Using movie ID, grab the cast member's ID
cast_0_ids = []
cast_1_ids = []

for movie_id in movie_ids:
    time.sleep(2)
    try:
        cast_0_ids.append(ia.get_movie(movie_id).get("cast")[0].getID())
        print("got 1 cast member")
    except:
        cast_0_ids.append(None)

for movie_id in movie_ids:
    time.sleep(2)    
    try:
        cast_1_ids.append(ia.get_movie(movie_id).get("cast")[1].getID())
    except:
        cast_1_ids.append(None)

#Using movie ID, grab the movie plot
plots=[]

for movie_id in movie_ids:
    time.sleep(2)
    try:
        plots.append(ia.get_movie(movie_id).get('plot')[0])
        print("got plot")
    except:
        plots.append(None)

#Create Movie DataFrame
movie_info = pd.DataFrame({"movie id": movie_ids, "movie names": movie_names, "plot": plots, 
                           "IMDb Rating": movie_ratings, 
                           "main cast member id": cast_0_ids, "main cast member name": cast_0_names, 
                           "other cast member id": cast_1_ids, "other cast member name": cast_1_names})

#Export as CSV file
movie_csv = "movies.csv"
movie_info.to_csv(movie_csv,index=False)


## *************** CAST TABLE ***************** ##
#Unite cast ids to create one list without duplicates
cast_ids = list(set().union(cast_0_ids, cast_1_ids))

#Create list of cast names and their latest role using cast IDs
cast_names = []

for cast_id in cast_ids:
    try:
        cast_names.append(ia.get_person(cast_id)["name"])
    except:
        cast_names.append(None)

latest_role = []

for cast_id in cast_ids:
    try:
        latest_role.append(ia.get_person(cast_id)["filmography"][0]["actor"][0]["title"])
    except:
        try:
            latest_role.append(ia.get_person(cast_id)["filmography"][0]["actress"][0]["title"])
        except:
            latest_role.append(None)

#Create cast DataFrame
cast_df = pd.DataFrame({"cast id": cast_ids, "cast name": cast_names, "latest role": latest_role})

#Export as CSV file
actor_csv = "cast.csv"
cast_df.to_csv(actor_csv,index=False)

## *************** TO POSTGRESQL ***************** ##
from config import username, password

#TABLE NAMES (SQL TABLE NAMES) & SET-UP
actors = 'actors_role'
movies = 'movies_info'
host = 'localhost'
port='5432'
database = 'moviedb'

## ******************** THE LOAD PANDAS DF METHOD *************************** ##
#START UP PostgreSQL
postgres_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(postgres_string)

#DEFINE FUNCTION(S)
def load_df(df, table):
    df.to_sql(table, engine)

#Load Pandas DF Using SQLAlchemy
# Movies Table
df = movie_info
table = actors
load_df(df,table)
print(f"{df} loaded into {table} successfully")

# Actors Table
df = cast_df
table = movies
load_df(df,table)
print(f"{df} loaded into {table} successfully")

 ## ********************* THE CSV FILE METHOD **************************** ##
'''
import psycopg2
conn = psycopg2.connect(database="postgres", user=username, password=password, host='localhost', port= '5432')
conn.autocommit = True    #or we could commit with conn.commit()
cursor = conn.cursor()


#function

def load_csv(csv_path, table):
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            cursor.execute(
            "INSERT INTO {table} VALUES (%s, %s, %s)",
            row
            )

#LOAD DATA TO PostgreSQL
#Load CSVs
#ACTORS
csv_path = actor_csv
table = actors
load_table(csv_path, table)
print(f"{csv_path} loaded into {table} successfully")

#MOVIES
csv_path = movie_csv
table = movies

load_table(csv_path, table)
print(f"{csv_path} loaded into {table} successfully")


#CLOSE CONNECTION
conn.close()

'''
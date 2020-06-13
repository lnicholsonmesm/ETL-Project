#Import dependencies
from bs4 import BeautifulSoup
import requests

#Grab response from Rotten Tomatoes URL
url = "https://editorial.rottentomatoes.com/guide/best-black-movies-21st-century/"
response = requests.get(url)
print(response.text)

#Create soup variable
soup = BeautifulSoup(response.content, 'html.parser')

#Search results in Header 2
results = soup.find_all("h2")

#Import pandas and IMDb libraries
import pandas as pd
from imdb import IMDb

#create instance of IMDb 
ia = IMDb()

#Loop through Rotten Tomatoes 100 Best Black Movies of the 21st Century to get their movie name on IMDb
movie_names=[]
for movie in movies:
    movie_results = ia.search_movie(movie)
    movie_names.append(movie_results[0]["title"])

#Using their IMDb name, grab these movies ids, ratings, and cast names
movie_ids = []


for movie_name in movie_names:
    movie_ids.append(movie_name.getID())

#Using their IMDb ID, grab their IMDb rating and first two cast members
movie_ratings=[]
cast_0_names = []
cast_1_names = []

for movie_id in movie_ids:
    try:
        rating = ia.get_movie(movie_id).get("rating")
        movie_ratings.append(rating)
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
    try:
        cast_0_ids.append(ia.get_movie(movie_id).get("cast")[0].getID())
    except:
        cast_0_ids.append(None)

for movie_id in movie_ids:
    try:
        cast_1_ids.append(ia.get_movie(movie_id).get("cast")[1].getID())
    except:
        cast_1_ids.append(None)


#Using movie ID, grab the movie plot
plots=[]

for movie_id in movie_ids:
    try:
        plots.append(ia.get_movie(movie_id).get('plot')[0])
    except:
        plots.append(None)

#Create Movie DataFrame

movie_info = pd.DataFrame({"movie id": movie_ids, "movie names": movie_names, "plot": plots, 
                           "IMDb Rating": movie_ratings, 
                           "main cast member id": cast_0_ids, "main cast member name": cast_0_names, 
                           "other cast member id": cast_1_ids, "other cast member name": cast_1_names})

#Export as CSV file
movie_info.to_csv("movies.csv",index=False)

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
cast_df.to_csv("cast.csv",index=False)
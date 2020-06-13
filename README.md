# ETL-Project
One Week Project - Final Report


EXTRACT: 
Our team used two sources of data for this ETL Project:
  - Rotten Tomatoes: We scrapped the site Rotten Tomatoes to grab the Top 100 Black Movies of the 21st Century. Since this data was scrapped, it was obtained using BeautifulSoup4.
  - IMDbPy: We used this Python package which is used to retrieve information of thee IMDb movie database. This was approved by Eric. By using this package we access the IMDb database through objects


TRANSFORM: 
We started this project by cleaning up the Rotten Tomatoes. One of the data entries hada. different release date in IMDb and Rotten Tomatoes, so we replaced it in Rotten Tomatoes with the IMDb data. Then we joined the data from Rotten Tomatoes with the IMDb database. Using these data, we pulled the rest of the information from Rotten Tomatoes, making them into two different tables.


LOAD: 
We used SQLAlchemy to load DataFrame into Postgres. The structure chosen was to have one schema with two tables, one for the movies and one for the actors. 
We used Postgres because it's open source and free. We also used it because it doesn't require to load csv files and we have experience in it. The structure was chosen to avoid data duplicates in the actor tables without taking data from the movie tables. 


conn
= psycopg2.connect
("dbname=suppliers user=postgres password=postgres")

CREATE DATABASE MovieDB;

testdb#=
create schema
if not exists ETL
CREATE TABLE actors
(
    id INT
    ,
    names VARCHAR
    ,
    last_role VARCHAR
)
CREATE VIEW tableview
AS
    SELECT names
    FROM actors
    WHERE names IS NOT NULL;





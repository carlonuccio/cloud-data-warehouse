#  Cloud Data Warehouse
***
## Udacity Data Engineer Nano Degree Project 3
***
### Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### The goal
***
The purpose of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


### Original Udacity Schema for Song Play Analysis
***
Using the song and log datasets, the projects wants a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table:
**songplays** - records in log data associated with song plays i.e. records with page **NextSong**\
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
**users** - users in the app\
*user_id, first_name, last_name, gender, level*\
**songs** - songs in music database\
*song_id, title, artist_id, year, duration*\
**artists** - artists in music database\
*artist_id, name, location, latitude, longitude*\
**time** - timestamps of records in songplays broken down into specific units\
*start_time, hour, day, week, month, year, weekday*


### Personal Schema for Song Play Analysis
***

<img src="starschema.png" alt="drawing" width="600"/>

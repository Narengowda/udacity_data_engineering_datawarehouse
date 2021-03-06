
# Project Summary of data ware house project

Sparkify is a startup company that has successfuly implemented an on-premise Data Warehouse in the past, satisfying all requirements from analytics team.

As time passed, more users joined Sparkify's streaming music service, leading the company to a more complex analytics and IT infrastructure.

The purpose of this project is to take the company to a whole new level where analyzing a massive amount of data is rapid and simple, and to enable worry-free of infrastructure scalability.
Cloud Data Warehouse

In order to embrace Sparkify's growth, the data engineering team reassessed the entire data analytics environment and came up with a new design for Cloud Data Warehouse.

## Data:
1. data/song_data : Contains metadata about a song and the artist of that song;
2. data/log_data : Consists of log files generated by the streaming app based on the songs in the dataset above;

Example data:
Song data is present in. EG:
    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

Logs at. Eg:
    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json


sample data
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Scripts Usage:

1. etl.py: Responsible for the entire data flow pipeline that will execute and extract data from JSON source files, transform data and load into Redshift tables;
2. create_table.py : Database and tables creation
3. sql_queries.py : Contains the creation table DDL and inserts DML scripts.




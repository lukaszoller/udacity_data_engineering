# UDACITY DWH PROJECT README

## Purpose of the Database

The purpose of this database is to assist Sparkify, a music streaming startup, in analyzing their user and song data. Analytical goals include understanding user behavior, identifying popular songs and artists, analyzing user demographics, and gaining insights into song plays over time. 

## Database Schema Design

The database follows a star schema design. The schema consists of:

### `songplays` Table

This fact table contains records of user song plays. It includes foreign keys linking to dimension tables and other information such as session details and user agents.

### Dimension Tables

1. `users`: Information about users, including user ID, names, gender, and user level.
2. `songs`: Details about songs, including song ID, title, artist ID, year, and duration.
3. `artists`: Artist-related information, including artist ID, name, location, latitude, and longitude.
4. `time`: Time-related information extracted from the timestamp in the `songplays` table, such as start time, hour, day, week, month, year, and weekday.

## ETL Pipeline

The ETL pipeline is designed to move data from Sparkify's JSON logs on user activity and song metadata in S3 to staging tables in Redshift. The process involves:

### Staging Tables

1. `staging_events`: Holds raw data from S3 related to user activity.
2. `staging_songs`: Holds raw data from S3 related to song metadata.

### Fact Table

1. `songplays`: Stores records of user song plays. Data is extracted and transformed from staging tables to fit the star schema.

### Dimension Tables

1. `users`
2. `songs`
3. `artists`
4. `time`

## Analytical Queries

There is a set of analytical queries for the Sparkify database. These queries are for validation of the different tables in the final tables.

## Runbook

This runbook guides you through the process of setting up the Sparkify database on Amazon Redshift and running the ETL pipeline.

### Files

| Filename         | Explanation                                                                                                 | 
|------------------|-------------------------------------------------------------------------------------------------------------|
| dwh.cfg          | Configuration for the connection to reshift cluster.                                                        | 
| etl.py           | Python code to extract data from s3, load to redshift and transform into final tables.                      | 
| etl_analytics.py | Some analytical queries for verification if the final tables have been created and data inserted correctly. | 
| sql_queries.py   | Definition of the queries to extract, transform and load the data.                                          |

### Prerequisites

1. Create an IAM Role according to  Udacity instructions. Write role name and ARN into dwh.cfg
2. Create Security Group
3. Launch Redshift Cluster: Write endpoint, db password and db user into dwh.cfg

### Database Setup

1. Run the following command to execute the ETL pipeline and load data into the database.
     ```bash
     python etl.py
     ```
2. Run the following command to verify that data has been written correctly into the final tables:
     ```bash
     python etl_analytics.py
     ```
   This query should print the following tables to console:

#### Songplays Table
| songplay_id | start_time           | user_id | level | song_id          | artist_id          | session_id | location                              | user_agent                                                            |
|-------------|----------------------|---------|-------|------------------|-------------------|------------|---------------------------------------|-----------------------------------------------------------------------|
| 0           | 2018-11-03 23:17:02  | 6       | free  | SOXTCXD12AB0183E39 | ARFL99B1187B9A2A45 | 5          | Atlanta-Sandy Springs-Roswell, GA     | Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0 |
| 2           | 2018-11-03 23:17:02  | 6       | free  | SOIWRGF12A8C1384C5 | ARFL99B1187B9A2A45 | 5          | Atlanta-Sandy Springs-Roswell, GA     | Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0 |
| 8           | 2018-11-08 01:38:19  | 11      | free  | SORDUJI12AB0183D3F | ARKU3Z61187FB51DCA | 10         | Elkhart-Goshen, IN                    | "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2" |
| 10          | 2018-11-08 01:38:19  | 11      | free  | SONQNPM12A67020A32 | ARKU3Z61187FB51DCA | 10         | Elkhart-Goshen, IN                    | "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2" |
| 16          | 2018-11-08 01:11:50  | 20      | paid  | SONAEJC12A8AE45BB4 | AR0KBXO1187B996460 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |
| 18          | 2018-11-08 01:15:02  | 20      | paid  | SOCOBMY12A58A7A161 | ARVHQNN1187B9B9FA3 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |
| 20          | 2018-11-08 01:15:02  | 20      | paid  | SONJNQI12A6310EDEE | ARVHQNN1187B9B9FA3 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |
| 22          | 2018-11-08 01:15:02  | 20      | paid  | SONCLNU12A6D4F86FB | ARVHQNN1187B9B9FA3 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |
| 24          | 2018-11-08 01:15:02  | 20      | paid  | SOLAYSZ12A6701F5BE | ARVHQNN1187B9B9FA3 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |
| 26          | 2018-11-08 01:15:02  | 20      | paid  | SOGWYVC12A6701F5DC | ARVHQNN1187B9B9FA3 | 19         | New York-Newark-Jersey City, NY-NJ-PA | "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36" |

#### Users Table

| user_id | first_name | last_name | gender | level |
|---------|------------|-----------|--------|-------|
| 2       | Jizelle    | Benjamin  | F      | free  |
| 3       | Isaac      | Valdez    | M      | free  |
| 4       | Alivia     | Terrell   | F      | free  |
| 5       | Elijah     | Davis     | M      | free  |
| 6       | Cecilia    | Owens     | F      | free  |
| 7       | Adelyn     | Jordan    | F      | free  |
| 8       | Kaylee     | Summers   | F      | free  |
| 9       | Wyatt      | Scott     | M      | free  |
| 10      | Sylvie     | Cruz      | F      | free  |
| 11      | Christian  | Porter    | F      | free  |

#### Songs Table

| song_id          | title                                          | artist_id          | year | duration |
|------------------|------------------------------------------------|-------------------|------|----------|
| SOAAAQN12AB01856D3 | Campeones De La Vida                           | ARAMIDF1187FB3D8D4 | 0    | 153      |
| SOAACTC12AB0186A20 | Christmas Is Coming Soon                       | ARXWFZ21187FB43A0B | 2008 | 180      |
| SOAADJH12AB018BD30 | Black Light (Album Version)                    | AR3FKJ61187B990357 | 1975 | 385      |
| SOAADUU12AB0183B6F | Intro / Locataire (Instrumental)               | AR70XXH1187FB44B55 | 0    | 101      |
| SOAAEHR12A6D4FB060 | Slaves & Bulldozers                            | AR5N8VN1187FB37A4E | 1991 | 415      |
| SOAAFHQ12A6D4F836E | Ridin' Rims (Explicit Album Version)           | AR3CQ2D1187B9B1953 | 2006 | 322      |
| SOAAFUV12AB018831D | Where Do The Children Play? (LP Version)       | AR5ZGC11187FB417A3 | 0    | 216      |
| SOAAHZO12A67AE1265 | Agni Sha Kshi                                  | AR9DE5T1187FB48CA3 | 0    | 229      |
| SOAAMWQ12A8C144DF1 | Happy Nation                                   | AR2IKF71187FB4D0C2 | 1992 | 255      |
| SOAANBW12A6D4F8DE9 | A Change Is Gonna Come (2001 Digital Remaster) | ARGF9VF1187FB37DAE | 1973 | 259      |

#### Artists Table

| artist_id          | name               | location               | latitude | longitude |
|---------------------|-------------------|------------------------|----------|-----------|
| AR00B1I1187FB433EB | Eagle-Eye Cherry   | Stockholm, Sweden       |          |           |
| AR00DG71187B9B7FCB | Basslovers United  |                        |          |           |
| AR00FVC1187FB5BE3E | Panda              | Monterrey, NL, México   | 25       | -100      |
| AR00JIO1187B9A5A15 | Saigon             | Brooklyn               | 40       | -73       |
| AR00LNI1187FB444A5 | Bruce BecVar       |                        |          |           |
| AR00MQ31187B9ACD8F | Chris Carrier      |                        |          |           |
| AR00TGQ1187B994F29 | Paula Toller       |                        |          |           |
| AR00Y9I1187B999412 | Akercocke          |                        |          |           |
| AR00YYQ1187FB504DC | God Is My Co-Pilot  | New York, NY            | 40       | -74       |
| AR016P51187B98E398 | Indian Ropeman     |                        |          |           |

#### Time Table

| start_time           | hour | day | week | month | year | weekday |
|----------------------|------|-----|------|-------|------|---------|
| 2018-11-01 21:01:46  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:05:52  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:08:16  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:11:13  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:17:33  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:24:53  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:28:54  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:42:00  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:52:05  | 21   | 1   | 44   | 11    | 2018 | 4       |
| 2018-11-01 21:55:25  | 21   | 1   | 44   | 11    | 2018 | 4       |

#### Top 5 Users with Most Songs Played

| user_id | first_name | last_name | song_count |
|---------|------------|-----------|------------|
| 49      | Chloe      | Cuevas    | 2050       |
| 80      | Tegan      | Levine    | 1952       |
| 15      | Lily       | Koch      | 1352       |
| 29      | Jacqueline | Lynch     | 1048       |
| 88      | Mohammad   | Rodriguez | 870        |

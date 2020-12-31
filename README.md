# Data modeling with Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

In this project I will create an Apache Cassandra database which can create queries on song play data to answer the questions.

## Datasets

The dataset `event_data` is stored in the directory [event_data](event_data/)   which contains CSV files partioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## ETL Pipeline

<p align="center">
<img src="images/pipeline.svg"/>
</p>

The figure above presents the ETL pipeline of project. It consists of three steps:

1. **E**xtract: Process CSV files in the directory [event_data](event_data/) to create a new CSV file.
2. **T**ransform: Create an Apache Cassandra database and transform data from the new CSV file into tables.
3. **L**oad: Do queries on database.

### 1. Process CSV files

In this part, we reads all CSV files in the directory [event_data](event_data/) then create a smaller event data file called `event_datafile_new.csv` that will be used to insert data into the Apache Cassandra tables.

The image below is a screenshot of what the denormalized data should appear like in the `event_datafile_new.csv` after the processing of CSV files.

<p align="center">
<img src="images/image_event_datafile_new.jpg"/>
</p>

### 2. Transform data into Cassandra database

The analysis team needs to query the database to answer the following three questions:
1. Get the artist, song title and song's length in the music app history that was heard during a session id and an item in section, for example, `sessionId = 338`, and `itemInSession = 4`.
2. Get only the following: name of artist, song (sorted by `itemInSession`) and user (first and last name) for a given user id and session id, for example, `userid = 10`, `sessionid = 182`.
3. Get every user name (first and last) in my music app history who listened to a given song, for example, who listened the song `All Hands Against His Own`.

#### Question 1: Get the artist, song title and song’s length in the music app history that was heard during a session id and an item in section.

To answer this question, we will create the table  `question_1`  which consists of 5 columns:

-   `artist`: stores the artist name, data type: text
-   `song`: stores the song title, data type: text
-   `length`: stores the song's length, data type: float
-   `session_id`: stores the session id, data type: int
-   `item_in_session`: stores the item number in session, data type: int

The partition key is  `session_id`, and the column key is  `item_in_session`.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS question_1 "
query += "(artist text, song text, length double, session_id int, item_in_session int, \
PRIMARY KEY (session_id, item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into table
with open(new_datafile, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO question_1 (artist, song, length, session_id, item_in_session)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))
```

#### Question 2: Get only the following: name of artist, song (sorted by `itemInSession`) and user (first and last name) for a given user id and session id.

To answer this question, we will create the table  `question_2`  which consists of 7 columns:

-   `artist`: stores the artist name, data type: text
-   `song`: stores the song title, data type: text
-   `session_id`: stores the session id, data type: int
-   `item_in_session`: stores the item number in session, data type: int
-   `user_id`: stores the user id, data type: int
-   `first_name`: stores the first name of user, data type: text
-   `last_name`: stores the last name of, data type: text

The partition key is  `user_id`, and the column keys are  `session_id`  and  `item_in_session`. Since we need songs sorted by item in section then  `item_in_session`  is added to column keys.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS question_2 "
query += "(artist text, song text, session_id int, item_in_session int, user_id int, first_name text, last_name text, \
PRIMARY KEY (user_id, session_id, item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into table
with open(new_datafile, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO question_2 (artist, song, session_id, item_in_session, user_id, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (line[0], line[9], int(line[8]), int(line[3]), int(line[10]), line[1], line[4])) 
```

#### Question 3: Get every user name (first and last) in my music app history who listened to a given song.

To answer this question, we will create the table  `question_3`  which consists of 4 columns:

-   `song`: stores the song title, data type: text
-   `user_id`: stores the user id, data type: int
-   `first_name`: stores the first name of user, data type: text
-   `last_name`: stores the last name of, data type: text

The partition key is  `song`, and the column key is  `user_id`. We add  `user_id`  to primary key to ensure the row data is unique.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS question_3 "
query += "(song text, user_id int, first_name text, last_name text, PRIMARY KEY (song, user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into table
with open(new_datafile, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO question_3 (song, user_id, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4])) 
```

### 3. Do queries on database

#### Question 1: Give me the artist, song title and song's length in the music app history that was heard during `sessionId = 338`, and `itemInSession = 4`.

```Python
query = "SELECT * FROM question_1 WHERE session_id=338 and item_in_session=4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print("artist: {}, song: {}, length: {}".format(row.artist, row.song, row.length))
```

The result:
```
artist: Faithless, song: Music Matters (Mark Knight Dub), length: 495.3073
```

#### Question 2: Give me only the following: name of artist, song (sorted by `itemInSession`) and user (first and last name) for `userid = 10`, `sessionid = 182`.

```Python
query = "SELECT * FROM question_2 WHERE user_id=10 and session_id=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print("artist: {}, song: {} (item in section: {}), user: {} {}".format(
            row.artist, row.song, row.item_in_session, row.first_name, row.last_name)
         )
```

The result:
```
artist: Down To The Bone, song: Keep On Keepin' On (item in section: 0), user: Sylvie Cruz
artist: Three Drives, song: Greece 2000 (item in section: 1), user: Sylvie Cruz
artist: Sebastien Tellier, song: Kilometer (item in section: 2), user: Sylvie Cruz
artist: Lonnie Gordon, song: Catch You Baby (Steve Pitron & Max Sanna Radio Edit) (item in section: 3), user: Sylvie Cruz
```

#### Question 3: Give me every user name (first and last) in my music app history who listened to the song `All Hands Against His Own`.

```Python
query = "SELECT * FROM question_3 WHERE song='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print("user: {} {}, user_id: {}".format(row.first_name, row.last_name, row.user_id))
```

The result:
```
user: Jacqueline Lynch, user_id: 29
user: Tegan Levine, user_id: 80
user: Sara Johnson, user_id: 95
```

The complete source code is in the notebook [project](project.ipynb).
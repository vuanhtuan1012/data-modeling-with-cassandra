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

To answer this question, we will create the table  `artist_song_session`  which consists of 5 columns:

-   `session_id`: stores the session id, data type: int
-   `item_in_session`: stores the item number in session, data type: int
-   `artist`: stores the artist name, data type: text
-   `song`: stores the song title, data type: text
-   `length`: stores the song's length, data type: float

The partition key is  `session_id`, and the column key is  `item_in_session`.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS artist_song_session "
query += "(session_id int, item_in_session int, artist text, song text, length double, \
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
        query = "INSERT INTO artist_song_session (session_id, item_in_session, artist, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```

#### Question 2: Get only the following: name of artist, song (sorted by `itemInSession`) and user (first and last name) for a given user id and session id.

To answer this question, we will create the table  `song_playlist_session`  which consists of 7 columns:

-   `user_id`: stores the user id, data type: int
-   `session_id`: stores the session id, data type: int
-   `item_in_session`: stores the item number in session, data type: int
-   `artist`: stores the artist name, data type: text
-   `song`: stores the song title, data type: text
-   `first_name`: stores the first name of user, data type: text
-   `last_name`: stores the last name of, data type: text

The partition keys are  `user_id` and `session_id`. We use both user_id and session_id as primary keys so that sessions from the same user are stored in the same nodes.

The column key is `item_in_session` as we need songs sorted by item in section.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS song_playlist_session "
query += "(user_id int, session_id int, item_in_session int, artist text, song text, first_name text, last_name text, \
PRIMARY KEY ((user_id, session_id), item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert data into table
with open(new_datafile, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_playlist_session (user_id, session_id, item_in_session, artist, song, \
                first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
```

#### Question 3: Get every user name (first and last) in my music app history who listened to a given song.

To answer this question, we will create the table  `user_song`  which consists of 4 columns:

-   `song`: stores the song title, data type: text
-   `user_id`: stores the user id, data type: int
-   `first_name`: stores the first name of user, data type: text
-   `last_name`: stores the last name of, data type: text

The partition key is  `song`, and the column key is  `user_id`. We add  `user_id`  to primary key to ensure the row data is unique.

```Python
# create table
query = "CREATE TABLE IF NOT EXISTS user_song "
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
        query = "INSERT INTO user_song (song, user_id, first_name, last_name)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))
```

### 3. Query data from database

#### Question 1: Give me the artist, song title and song's length in the music app history that was heard during `sessionId = 338`, and `itemInSession = 4`.

```Python
query = "SELECT artist, song, length FROM artist_song_session WHERE session_id=338 and item_in_session=4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

data = PrettyTable()
data.field_names = ["Artist", "Song", "Length"]
data.align = "l"
data.align["Length"] = "r"
for row in rows:
    data.add_row([row.artist, row.song, row.length])
print(data)
```

The result:

| Artist | Song | Length |
| :--- | :--- | ---: |
| Faithless | Music Matters (Mark Knight Dub) | 495.3073 |

#### Question 2: Give me only the following: name of artist, song (sorted by `itemInSession`) and user (first and last name) for `userid = 10`, `sessionid = 182`.

```Python
query = "SELECT item_in_session, artist, song, first_name, last_name FROM song_playlist_session \
WHERE user_id=10 and session_id=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

data = PrettyTable()
data.field_names = ["Artist", "Song", "Item in session", "First name", "Last name"]
data.align = "l"
data.align["Item in session"] = "r"
for row in rows:
    data.add_row([row.artist, row.song, row.item_in_session, row.first_name, row.last_name])
print(data)
```

The result:

| Artist | Song | Item in session | First name | Last name |
| :--- | :--- | ---: | :--- | :--- |
| Down To The Bone | Keep On Keepin' On | 0 | Sylvie | Cruz |
| Three Drives | Greece 2000 | 1 | Sylvie | Cruz |
| Sebastien Tellier | Kilometer | 2 | Sylvie | Cruz |
| Lonnie Gordon | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | 3 | Sylvie | Cruz |

#### Question 3: Give me every user name (first and last) in my music app history who listened to the song `All Hands Against His Own`.

```Python
query = "SELECT user_id, first_name, last_name FROM user_song WHERE song='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

data = PrettyTable()
data.field_names = ["User Id", "First name", "Last name"]
data.align = "l"
data.align["User Id"] = "r"
for row in rows:
    data.add_row([row.user_id, row.first_name, row.last_name])
print(data)
```

The result:

| User Id | First name | Last name |
| ---: | :--- | :--- |
| 29 | Jacqueline | Lynch |
| 80 | Tegan | Levine |
| 95 | Sara | Johnson |

The complete source code is in the notebook [project](project.ipynb).
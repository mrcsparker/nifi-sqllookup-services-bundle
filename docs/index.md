---
layout: default
title: Documentation
---

NiFi SQL Lookup Services Bundle allows you to so SQL for your NiFi LookupRecord and LookupAttribute needs.

For background, read:

* [Data flow enrichment with NiFi part 1 : LookupRecord processor](https://community.hortonworks.com/articles/138632/data-flow-enrichment-with-nifi-lookuprecord-proces.html)
* [Data flow enrichment with NiFi part 2 : LookupAttribute processor](https://community.hortonworks.com/articles/140231/data-flow-enrichment-with-nifi-part-2-lookupattrib.html)
* [Data flow enrichment with NiFi part 3: LookupRecord with MongoDB](https://community.hortonworks.com/articles/146198/data-flow-enrichment-with-nifi-part-3-lookuprecord.html)

Compiling
==========

Before you use the service you are going to need to compile it.

__First__ grab the source code from github:

```sh
>> git clone https://github.com/mrcsparker/nifi-sqllookup-services-bundle.git
```

__Second__ compile the code:

```sh
>> cd nifi-sqllookup-services-bundle
>> mvn package
```

__Third__ add the compiled NiFi nars to your local NiFi install:

```sh
>> cp ./nifi-sqllookup-services-nar/target/nifi-sqllookup-services-nar-1.10.0.nar /PATH/TO/NIFI/lib
>> cp ./nifi-sqllookup-services-api-nar/target/nifi-sqllookup-services-api-nar-1.10.0.nar /PATH/TO/NIFI/lib
```
__Finally__ startup NiFi:

```sh
>> cd /PATH/TO/NIFI
>> ./bin/nifi.sh run
```

You are ready to go!

Using the SQL Lookup Services Bundle
====================================

There are two NiFi controllers in the SQL Lookup Services bundle:

1. _LookupAttribute_: look up a single column from a SQL query and assign it as an attribute to a FlowFile
2. _LookupRecord_: look up an entire row from a SQL query and add it to the contents of a FlowFile

In this case, we are going to go over the _LookupRecord_ controller (_SQLRecordLookupService_).

We are going to use PostgreSQL for the backend data store and the [MovieLens](https://grouplens.org/datasets/movielens/) data.

### 1. Download the MovieLens `ml-latest.zip` file

### 2. Create a database named `movielens`

### 3. Create the following schemas in `movielens`:

```sql
create table movies (
    movie_id	int,
    title		varchar(200),
    genres		varchar(1000),
    primary key(movie_id)
);

create table ratings (
    user_id		int,
    movie_id	int,
    rating		float,
    timestamp	bigint,
    primary key(user_id, movie_id)
);

create table tags (
    user_id		int,
    movie_id	int,
    tag			varchar(255),
    timestamp	bigint,
    primary key(user_id, movie_id, tag)
);

create table links (
  movie_id int,
  imdb_id int,
  tmdb_id int,
  primary key(movie_id)
);

create table genome_tags (
  tag_id int,
  tag varchar(100),
  primary key(tag_id)
);

create table genome_scores (
  movie_id int,
  tag_id int,
  relevance float,
  primary key(movie_id, tag_id)
);
```

### 4. Unzip and, using psql, copy in the MovieLens data:

```sh
movielens=# \copy movies from './ml-latest/movies.csv' delimiter ',' CSV header;
movielens=# \copy ratings from './ml-latest/ratings.csv' delimiter ',' CSV header;
movielens=# \copy tags from './ml-latest/tags.csv' delimiter ',' CSV header;
movielens=# \copy links from './ml-latest/links.csv' delimiter ',' CSV header;
movielens=# \copy genome_tags from './ml-latest/genome-tags.csv' delimiter ',' CSV header;
movielens=# \copy genome_scores from './ml-latest/genome-scores.csv' delimiter ',' CSV header;
```






 

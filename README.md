# Apache NiFi SQL Lookup Service

[![Build Status](https://travis-ci.org/mrcsparker/nifi-sqllookup-services-bundle.svg?branch=master)](https://travis-ci.org/mrcsparker/nifi-sqllookup-services-bundle)

<!-- TOC -->

- [Apache NiFi SQL Lookup Service](#apache-nifi-sql-lookup-service)
  - [About](#about)
  - [Simple Setup](#simple-setup)
  - [SQL Query Support](#sql-query-support)
  - [Caching](#caching)
    - [Supported caches](#supported-caches)
  - [Latest release](#latest-release)
  - [Articles on using NiFi lookup services](#articles-on-using-nifi-lookup-services)

<!-- /TOC -->

## About

**NiFI SQL Lookup Service** is a SQL-based lookup service for [Apache NiFi](https://nifi.apache.org). It allows you to enrich your flowfiles with any jdbc-compliant data store, including:

- PostgreSQL
- Oracle
- MySQL
- MS SQL Server
- SQLite
- ... and many more

It includes both [LookupRecord](http://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.6.0/org.apache.nifi.processors.standard.LookupRecord/) and [LookupAttribute](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.6.0/org.apache.nifi.processors.standard.LookupAttribute/) controllers.

These controllers were designed to be _flexible_ and _fast_.

## Simple Setup

- [Download Apache NiFi](https://nifi.apache.org/download.html)
- Compile and install `nifi-sqllookup-bundle`

```bash
> cd nifi-sqllookup-bundle
> mvn package
> cp ./nifi-sqllookup-services-nar/target/nifi-sqllookup-services-nar-1.15.0.nar /NIFI_INSTALL/lib/
> cp ./nifi-sqllookup-services-api-nar/target/nifi-sqllookup-services-api-nar-1.15.0.nar /NIFI_INSTALL/lib/
```

- Start NiFi

## SQL Query Support

This service supports multiple query types:

- _Named Parameters_: `SELECT name FROM foo WHERE value = :value`
- _SQL IN queries_: `SELECT name FROM foo WHERE value IN(:values)`
- _Multiple lookup values_: `SELECT name FROM foo WHERE value IN(:values) AND sequence = :sequence AND catalog = :catalog`

## Caching

The goal of this service is to return values quickly. It has a built-in cache so that your database doesn't get overwhelmed.

This is configurable in the controller settings.

### Supported caches

These caches are all built-in to this service. Select your preferable cache in the controller settings.

- [Caffeine](https://github.com/ben-manes/caffeine) - default cache.
- [Cache2k](https://cache2k.org)
- [Guava](https://github.com/google/guava/wiki/CachesExplained)

You can also select the number of items that you want to cache. The caches all keep the most accessed items available by default.

If you don't know which to choose, just go with the default.

## Latest release

[https://github.com/mrcsparker/nifi-sqllookup-services-bundle/releases/latest](https://github.com/mrcsparker/nifi-sqllookup-services-bundle/releases/latest)

## Articles on using NiFi lookup services

* [Data flow enrichment with NiFi part 1 : LookupRecord processor](https://community.hortonworks.com/articles/138632/data-flow-enrichment-with-nifi-lookuprecord-proces.html)
* [Data flow enrichment with NiFi part 2 : LookupAttribute processor](https://community.hortonworks.com/articles/140231/data-flow-enrichment-with-nifi-part-2-lookupattrib.html)
* [Data flow enrichment with NiFi part 3: LookupRecord with MongoDB](https://community.hortonworks.com/articles/146198/data-flow-enrichment-with-nifi-part-3-lookuprecord.html)

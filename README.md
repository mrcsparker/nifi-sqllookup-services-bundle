# Apache NiFi SQL Lookup Service

[![Build Status](https://travis-ci.org/mrcsparker/nifi-sqllookup-services-bundle.svg?branch=master)](https://travis-ci.org/mrcsparker/nifi-sqllookup-services-bundle)

**NiFI SQL Lookup Service** is a SQL-based lookup service. It allows you to enrich your flowfiles with any jdbc-compliant data store.

It includes LookupRecord and LookupAttribute controllers.

These controllers were designed to be flexible and fast. They support:

* _Simple SQL queries_: `SELECT name FROM foo WHERE value = ?`
* _Named Parameters_: `SELECT name FROM foo WHERE value = :value`
* _SQL IN queries_: `SELECT name FROM foo WHERE value IN(:values)`
* _Multiple lookup values_: `SELECT name FROM foo WHERE value IN(:values) AND sequence = :sequence AND catalog = :catalog`
* _Caching_: LookupRecord and LookupAttribute both keep a cache of the most accessed objects. This is configurable in the controller settings.

*Multiple lookup values, in-queries, and named parameters as supported if the lookup `key` is in JSON format*

## Simple Setup

* [Download Apache NiFi](https://nifi.apache.org/download.html)

* Compile and install `nifi-sqllookup-bundle`

```bash
> cd nifi-sqllookup-bundle
> mvn package
> cp ./nifi-sqllookup-services-nar/target/nifi-sqllookup-services-nar-1.6.0.nar /NIFI_INSTALL/lib/
> cp ./nifi-sqllookup-services-api-nar/target/nifi-sqllookup-services-api-nar-1.6.0.nar /NIFI_INSTALL/lib/
```

* Start NiFi

## Full Documentation

[https://mrcsparker.github.io/nifi-sqllookup-services-bundle](https://mrcsparker.github.io/nifi-sqllookup-services-bundle)

## Latest release

[https://github.com/mrcsparker/nifi-sqllookup-services-bundle/releases/latest](https://github.com/mrcsparker/nifi-sqllookup-services-bundle/releases/latest)

## Articles on using NiFi lookup services

* [Data flow enrichment with NiFi part 1 : LookupRecord processor](https://community.hortonworks.com/articles/138632/data-flow-enrichment-with-nifi-lookuprecord-proces.html)
* [Data flow enrichment with NiFi part 2 : LookupAttribute processor](https://community.hortonworks.com/articles/140231/data-flow-enrichment-with-nifi-part-2-lookupattrib.html)
* [Data flow enrichment with NiFi part 3: LookupRecord with MongoDB](https://community.hortonworks.com/articles/146198/data-flow-enrichment-with-nifi-part-3-lookuprecord.html)





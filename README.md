Pustike Persist   [![][Maven Central img]][Maven Central] [![][Javadocs img]][Javadocs] [![][license img]][license]
===============
Persist is an object based persistence library. Using a simple configuration and annotation based entity mapping, it provides features like schema generation, sql query api to insert / update / delete objects and a fluent data finder api.

Using [Java Persistence API](https://en.wikipedia.org/wiki/Java_Persistence_API) is the standard approach to object persistence in Java based applications. And it is a comprehensive and complex API to implement and use. Using a JPA implementation for persistence can add lot of overhead to small/medium sized applications. So, Pustike Persist provides a simple api to perform common SQL queries using object metadata built using JPA like annotations.

Following are some of its key features:

* Repository configuration using dataSource and schema metadata
* Entity mapping to database table and fields to columns using annotations
* Database schema generation with index, foreign keys, constraints, etc using the mapping tool
* Allows field group definitions per entity to fetch or update only the required data
* Insert single/list of objects into database in batch and support update on conflict using excluded rows
* Update single/multiple objects in the database using the specified field group
* Select row data as object with the given identity and option to select them for update with lock
* Delete single/multiple objects
* A fluent Finder API to construct sql queries and fetch result data as objects 
* Supported databases: PostgreSQL
* Requires Java 11 and has no external dependencies (~60kB in size)

**Documentation:** Latest javadocs is available [here](https://pustike.github.io/pustike-persist/docs/api/).

**Todo:**

* Write unit tests
* More detailed documentation
* Override field name when using field@fgName instead of adding both
* Finder API - add fetch(String fgName) as a method to reduce to overloaded fetch methods
* MappingTool - create ForeignKey column based on the type of @Id column
* MappingTool - if existing field metadata is changed, compare with database and show warnings 
* Add support for Composite primary key
* Implement L1 Cache to avoid creating same object multiple times
* Support more databases (MySQL, H2, HSQLDB, Derby)

Download
--------
To add a dependency using Maven, use the following:
```xml
<dependency>
    <groupId>io.github.pustike</groupId>
    <artifactId>pustike-persist</artifactId>
    <version>0.9.0</version>
</dependency>
```
To add a dependency using Gradle:
```
dependencies {
    compile 'io.github.pustike:pustike-persist:0.9.0'
}
```
Or, download the [latest JAR](https://search.maven.org/remote_content?g=io.github.pustike&a=pustike-persist&v=LATEST)

License
-------
This library is published under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

[Maven Central]:https://maven-badges.herokuapp.com/maven-central/io.github.pustike/pustike-persist
[Maven Central img]:https://maven-badges.herokuapp.com/maven-central/io.github.pustike/pustike-persist/badge.svg

[Javadocs]:https://javadoc.io/doc/io.github.pustike/pustike-persist
[Javadocs img]:https://javadoc.io/badge/io.github.pustike/pustike-persist.svg

[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202-blue.svg

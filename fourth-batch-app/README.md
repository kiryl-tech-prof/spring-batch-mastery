# Prerequisites

In order to run this Spring Batch application, the following pre-requisites need to be met

## Java 17

Java of version 17.
To check: `java --version`

Which should give result similar to
```shell
java 17.0.9 2023-10-17 LTS
Java(TM) SE Runtime Environment (build 17.0.9+11-LTS-201)
Java HotSpot(TM) 64-Bit Server VM (build 17.0.9+11-LTS-201, mixed mode, sharing)
```

## Gradle 8

Ideal version is **8.4**, though versions close enough to 8.4 should work as well.
To check: `gradle --version`

The desired result is similar to
```shell
------------------------------------------------------------
Gradle 8.4
------------------------------------------------------------

Build time:   2023-10-04 20:52:13 UTC
Revision:     e9251e572c9bd1d01e503a0dfdf43aedaeecdc3f

Kotlin:       1.9.10
Groovy:       3.0.17
Ant:          Apache Ant(TM) version 1.10.13 compiled on January 4 2023
JVM:          17.0.9 (Oracle Corporation 17.0.9+11-LTS-201)
OS:           Mac OS X 11.7.10 x86_64
```

## Postgresql 15.x

Ideal version is **15.4** (the one it has been tested with), however versions close enough to 15.4 should work as well.
Postgres is used in this application as an input & output data source, and also in unit tests.
To check, connect to your instance: `psql -U yourusername -W` (default superuser name is **postgres**)
And in Postgres terminal, type: `select version();`

The desired result should be similar to
```shell
                                                     version                                                      
------------------------------------------------------------------------------------------------------------------
 PostgreSQL 15.4 on x86_64-apple-darwin20.6.0, compiled by Apple clang version 12.0.5 (clang-1205.0.22.9), 64-bit
(1 row)
```

**!!!** There is an important step to create an empty database to be used as a source of data for the application:
```shell
create database mydatabaseforapp;
```

## MySQL 8.0.x

Ideal version is **8.0.31** (the one it has been tested with), however versions close enough to 8.0.31 should work as well.
MySQL is used in this application as a job repository storage for Spring Batch metadata.
To check, connect to your instance: `mysql -u yourusername -h yourhostifnotlocal -p`
And in MySQL terminal, type: `select version();`

The desired result is similar to
```shell
+-----------+
| version() |
+-----------+
| 8.0.31    |
+-----------+
```

**!!!** One important step is to create empty database, which is easy to do from MySQL command-line:
```shell
create database mydatabasename;
```

# How-to

## Common Configurations

In order to run jobs of this Spring Batch application, there are properties that need to be set up in **src/res/db.properties and src/res/source.properties** files, according to DB names, usernames and passwords that you configured.
`db.url`, `db.username` and `db.password` properties need to be setup to configure Spring Batch to access MySQL job repository metadata db in **src/res/db.properties**. For example:
```properties
db.url=jdbc:mysql://localhost:3306/jobrepodbname
db.username=root
db.password=kirylbatchpassword
```

Corresponding `db.src.url`, `db.src.username` and `db.src.password` properties need to be setup to configure Postgresql database to be an input / output source in **src/res/source.properties**. For example:
```properties
db.src.url=jdbc:postgresql://127.0.0.1:5432/sourcedbname
db.src.username=postgres
db.src.password=sourcepassword
```

## Build

Before generating the data and / or starting the application, the application needs to be built:
```shell
gradle clean build
```

## Generate input data

In case all configuration properties are set properly (mentioned above), and application is built (previous section) input data in Postgresql could be generated. The following command need to be run from the root directory of the project:
```shell
gradle generateData
```

The command will delete the old data first, and then generate the new one.

## Application start

In order to start the application to run on 8080 port, the following command needs to be run:
```shell
gradle bootRun --args='--server.port=8080'
```

Port could be changed, however please account for that once you try to call application's endpoints.
For example, the following command will start the application to run on 8181 port:
```shell
gradle bootRun --args='--server.port=8181'
```

## Starting the job

In order to start the jobs provided by the application, the following commands need to be run in the corresponding use cases.
All the use cases mentioned below are started asynchronously through HTTP, using `curl` command line utility. You are free to use any alternative utility of your choice.
 - Single thread job: `curl -X POST http://localhost:8080/start-simple-local`
 - Multithreaded job: `curl -X POST http://localhost:8080/start-multi-threaded`. Please keep in mind that this specific job will produce incorrect results in most cases because of out-of-order nature of handling records with multiple threads. It is available for Spring Batch capabilities demonstration purposes only
 - Partitioned local (threads) job: `curl -X POST http://localhost:8080/start-partitioned-local`
 - Partitioned remote job is **discussed below**

## Operating the partitioned remote job

In order to start partitioned remote job, `worker.server.base.urls` property in **src/res/partitioning.properties** needs to be set up properly.
Property should contain base URLs to active & ready-to-serve-requests servers / applications. Each server / application will handle one partition (distributed based on user id) after it's started.
For example, I can run the calculation with distributing the load over 3 applications: http://localhost:8080/, http://localhost:8181/ and http://localhost:8282/ (assuming all of them are up, running, and ready to handle requests)
In this case, `worker.server.base.urls` property in **src/res/partitioning.properties** would look like
```properties
worker.server.base.urls=http://localhost:8080/,http://localhost:8181/,http://localhost:8282/
```

Please keep in mind that application does not care whether base URL is localhost, or any other domain name / IP address available through the Internet / network. The main thing for all applications is to run the same (this one) Spring Batch application.

In order to start the distributed calculation, the following HTTP request needs to be sent to any of the configured servers / applications: `curl -X POST http://localhost:8080/start-partitioned-remote`
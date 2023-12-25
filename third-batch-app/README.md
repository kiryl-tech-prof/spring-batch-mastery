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

## MySQL 8.0.x

Ideal version is **8.0.31** (the one it has been tested with), however versions close enough to 8.0.31 should work as well.
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

## Configurations

In order to run the team performance job, there are 3 properties that needs to be adjusted in **src/res/db.properties** file: `db.url`, `db.username` and `db.password`. Assign the values that you configured. For example:

```properties
db.url=jdbc:mysql://localhost:3306/mydatabasename
db.username=root
db.password=kirylbatchpassword
```

## Build & run

The following lifecycle happens when operating this application:
1. Application needs to be built, meaning source files, configurations, etc.
2. Application needs to be started as a server application
3. Server application can be used through the endpoint to start Spring Batch jobs

### Build

In order to build the application, please run the following command from the root of the project: `gradle clean build`

### Server start

In order to start application server, please run the following command from the root of the project: `gradle bootRun --args='--server.port=8080'`

### Starting the job

To start the team performance job with specific score rank in mind, please run the following command to send HTTP POST request to the application server: `curl -X POST http://localhost:8080/start?scoreRank=0`

### Tests

To only run tests of the application, please run the following command from the root of the project: `gradle clean test`
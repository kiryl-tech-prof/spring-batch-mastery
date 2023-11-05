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

## (Optional) uuidgen

Optionally, you can install **uuidgen** utility. On Mac, it should be automatically installed. To check: `uudgen` in your terminal, it should type unique id similar to
```shell
93375C08-CE4B-435D-BB29-B9130D7F93A5
```

In case you do not have it installed, it's OK as well. You will need to manually change parameters of the command line utility, as mentioned further

# How-to

## Configurations

In order to run the temperature sensor job, there are 3 properties that needs to be adjusted in **src/res/db.properties** file: `db.url`, `db.username` and `db.password`. Assign the values that you configured. For example:

```properties
db.url=jdbc:mysql://localhost:3306/mydatabasename
db.username=root
db.password=kirylbatchpassword
```

## Build

From the root of the project (folder **first**), run `gradle clean buildJar`

## Run temperature sensor job

**Please build the code before running it (!)**

In case you have **uuidgen** installed, use the following command to (re-)run th job:
```shell
java -jar build/libs/first-batch-app-final.jar org.example.TemperatureSensorRootConfiguration temperatureSensorJob id=$(uuidgen)
```

If you do not have it, you need to place unique number in the last (id) parameter specified, and **do it every time you run**. Otherwise, Spring Batch will not re-run the job. Example:
```shell
java -jar build/libs/first-batch-app-final.jar org.example.TemperatureSensorRootConfiguration temperatureSensorJob id=23
```
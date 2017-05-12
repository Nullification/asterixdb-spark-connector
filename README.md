AsterixDB Spark Connector
============================
This an Apache Spark connector which allows it to consume data from Apache AstreixDB.

SQL++ ONLY
---
Currenlty the connector supports SQL++ only. AQL support should be available soon... 

Requirements
---
- Java version 8
- SBT version >= 0.3.17
- Scala 2.10 (Never tested on 2.11)
- Apache AsterixDB version >= 0.9.2 (up and running)

Build
---
- Clone and go the asterixdb-spark-connector directory.
- Check ``project/Versions.scala`` if the specified versions of Apache Spark and Apache Hyracks are not similar to what you have.
- run:
```
sbt package
```
- Assembly JAR:
```
# The JAR file should be located at target/scala-2.10/asterixdb-spark-connector-assembly-SPARK_VERSION.jar
sbt assembly
```
- Publish to local repo:
```
sbt publish
```

Configuration
---
- asterixdb-spark-connector requires to be configure using SparkConf (See the example for clarification):

Property Name | Default | Description
--- | --- | ---
spark.asterix.connection.host | None | IP/Host of AsterixDB.
spark.asterix.connection.port | None | Port for AsterixDB HTTP API (Default: 19002).
spark.asterix.frame.size | None | The frame size of AsterixDB can be found in conf/asterix-configuration.xml file under Managix folder.

- Optional configurations:

Property Name | Default | Description
--- | --- | ---
spark.asterix.frame.number | 2 | Number of AsterixDB frames to read at a time. <br> This should NOT be big as the intermediate result can consume large amount memory.
spark.asterix.reader.number | 2 | The number of parallel readers per AsterixDB result partition.
spark.asterix.prefetch.threshold | 2 | The remaining number of unread tuples before trigger the pre-fetcher. <br> This should NOT be big as the intermediate result can consume large amount memory.

Using the connector in your code
---
#### Using SBT and Maven
- Build ``asterixdb-spark-connector`` and publish it to your local-repo: 
```
sbt publish
```
- Add SBT dependency to your ``build.sbt``
```
libraryDependencies += "org.apache.asterix" %% "asterixdb-spark-connector" % $SPARK_VERSION
```
- or add Maven dependency to your ``pom.xml``
```
<dependency>
        <groupId>org.apache.asterix</groupId>
        <artifactId>asterixdb-spark-connector_2.10</artifactId>
        <version>$SPARK_VERSION</version>
</dependency>
```
- Replace ``$SPARK_VERSION`` with the required Apache Spark version


#### Using <b>spark-shell</b>
---
- Using local-repo to get the connector:
```bash
bin/spark-shell [--master Spark master address or ignore for local run] \\
--packages org.apache.asterix:asterixdb-spark-connector_2.10:SPARK_VERSION \\ 
--conf spark.asterix.connection.host=ASTERIXDB_HOST \\
--conf spark.asterix.connection.port=ASTERIXDB_PORT \\
--conf spark.asterix.frame.size=ASTERIXDB_FRAME_SIZE \\
```
- Using Assembly JAR:
```
./bin/spark-shell --jars path/to/jar/asterixdb-spark-connector-assembly-SPARK_VERSION.jar
--conf spark.asterix.connection.host=ASTERIXDB_HOST \\
--conf spark.asterix.connection.port=ASTERIXDB_PORT \\
--conf spark.asterix.frame.size=ASTERIXDB_FRAME_SIZE \\
```

Example
Please see the following:
##### [SQL++ example.](https://github.com/Nullification/asterixdb-spark-connector/blob/master/src/main/scala/org/apache/asterix/connector/example/Example.scala)
##### Using [Apache Zeppelin.](https://zeppelin.apache.org)
-  [Load zeppelin-notebook example.](https://github.com/Nullification/asterixdb-spark-connector/tree/master/zeppelin-notebook/asterixdb-spark-example)

##### Add the connector to Spark Interpreter in Apache Zeppelin

###### Click Interpreter
 ![alt text](https://raw.githubusercontent.com/Nullification/asterixdb-spark-connector/master/zeppelin-notebook/1.png "AsterixDB-Spark Connector with Apache Zeppelin")
 
###### Scroll down to ```Spark``` and add the configurations for the connector 
 ![alt text](https://raw.githubusercontent.com/Nullification/asterixdb-spark-connector/master/zeppelin-notebook/2.png "AsterixDB-Spark Connector with Apache Zeppelin")
 
###### Add the connector artifact as ```groupId:artifactId:version```
 ![alt text](https://raw.githubusercontent.com/Nullification/asterixdb-spark-connector/master/zeppelin-notebook/3.png "AsterixDB-Spark Connector with Apache Zeppelin")


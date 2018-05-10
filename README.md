# G-CORE interpreter

## To build and run
The project will build successfully under Java 8. Spark 2.2.0 is needed to run
the application. You can download Spark from https://spark.apache.org/downloads.html.

```bash
mvn package -DskipTests
spark-submit --class GcoreRunner --master local[2] target/gcore-interpreter-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## To run tests

```bash
mvn test
```

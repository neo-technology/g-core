# G-CORE interpreter

## To build and run
The project will build successfully under Java 8. Spark 2.2.0 is needed to run
the application. You can download Spark from
https://spark.apache.org/downloads.html.

To sumbit on Spark, we need to package an uber-jar from the project's sources.
To avoid running the tests when packaging the uber-jar, you can add the
-DskipTests flag to the mvn package command.

As the Spoofax parser uses Guice as a dependency injection framework, we need to
pass the Guice 4.0 jar separately to the driver as a spark.driver.extraClassPath
property.

```bash
mvn package [-DskipTests]
spark-submit \
    --class GcoreRunner \
    --master local[2] \
    --conf "spark.driver.extraClassPath=/path_to/guice-4.0.jar" \
    target/gcore-interpreter-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## To run tests

```bash
mvn test
```

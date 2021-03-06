# Spark Scheduling Demo

This repository contains an example of starting multiple Spark jobs in parallel. This code was used for demo in a webinar which can be accessed here https://www.youtube.com/watch?v=e7qbrpvmOgY

In order to build the project you will need `sbt` which you cen get on https://www.scala-sbt.org/.
Then you can build with
```bash
sbt compile
sbt package
```

In order to start the Spark application you will need to install Spark. This repo uses Spark 2.4.5, if you have 
an earlier version, you can change the variable `sparkVersion` in `build.sbt`.

The application contains two runnable classes: `SimpleJob` and `ParallelJob`

You can run any of it by:
 
 ```bash
 spark-submit \
 --master local \
 <other configs> \
 --class <SimpleJob or ParallelJob>  \
 <path to jar> \
 <path to data>
```

## Data

Data comes from a training set for this kaggle competition https://www.kaggle.com/c/nlp-getting-started/data

## Want to read more?

Very useful example I also recycled
https://www.waitingforcode.com/apache-spark/fair-jobs-scheduling-apache-spark/read

Official Documentation
https://spark.apache.org/docs/latest/job-scheduling.html


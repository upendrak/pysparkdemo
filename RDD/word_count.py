#!/usr/bin/env python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD basics").getOrCreate()

RDD3 = spark.sparkContext.textFile('README.md', 4)

words = RDD3.flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y) \
        .map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

# Save the output as 1 partition
words.coalesce(1).saveAsTextFile("output")
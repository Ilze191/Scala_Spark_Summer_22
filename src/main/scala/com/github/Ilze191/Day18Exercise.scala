package com.github.Ilze191

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object Day18Exercise extends App {
  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val flightData2014 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2014-summary.csv")

  println(s"We have ${flightData2014.count()} rows of data")

  flightData2014.createOrReplaceTempView("flight_data_2014")

  //TODO open up flight Data from 2014
  //TODO create SQL view

  //TODO ORDER BY flight counts
  //TODO show top 10 flights

  val maxFlights = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as flight_counts
FROM flight_data_2014
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
""")

  maxFlights.show(10)




}

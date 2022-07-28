package com.github.Ilze191

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col

import scala.util.Random

object Day22Exercise extends App {
  val spark = SparkUtil.getSpark("Sparky")

  //TODO open up 2014-summary.json file
  val flightPath = "src/resources/flight-data/json/2014-summary.json"
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)

  //TODO Task 1 - Filter only flights FROM US that happened more than 10 times
  df.where(col("ORIGIN_COUNTRY_NAME") === "United States")
    .where(col("count") > 10)
    .show(5)

  //TODO Task 2 - I want a random sample from all 2014 of roughly 30 percent, you can use a fixed seed
  //subtask I want to see the actual row count
  val seed = 6
  val withReplacement = false
  val fraction = 0.3
  val dfSample = df.sample(withReplacement, fraction, seed)
  dfSample.show(5)
  println(s"We got ${dfSample.count()} samples")

  //TODO Task 3 - I want a split of full 2014 dataframe into 3 Dataframes with the following proportions 2,9, 5
  //subtask I want to see the row count for these dataframes and percentages
  val dataFrames = df.randomSplit(Array(2, 9, 5), seed)

  for ((dFr, i) <- dataFrames.zipWithIndex) {
    println(s"DataFrame No. $i has ${dFr.count} rows")
  }

val dPercentages= dataFrames.map(d => d.count() * 100 / df.count())

  println(s"DataFrame is split by percentages --> ${dPercentages.mkString(", ")}")




}

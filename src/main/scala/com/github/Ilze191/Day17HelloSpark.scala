package com.github.Ilze191

import org.apache.spark.sql.SparkSession

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

//  val myRange = spark.range(1000).toDF("number") //create a single column dataframe (table)
//  val divisibleBy5 = myRange.where("number % 5 = 0") //so similaraities with SQL and regular Scala
//  divisibleBy5.show(10) //show first 10 rows

  //TODO create range of numbers 0 to 100
  val range = spark.range(100).toDF("number")
  //TODO filter into numbers divisible by 10
  val divisibleBy10 = range.where("number % 10 = 0")
  //TODO show the results
  divisibleBy10.show
  spark.stop()

}

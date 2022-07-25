package com.github.Ilze191

import org.apache.spark.sql.SparkSession

object Day20StructuredAPI extends App{
  println("CH4: Structured API Overview - look into DataFrames and Datasets")

  println(s"Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version ${spark.version}")

  // in Scala . No saving just showing
  val df = spark.range(40).toDF("number") //create a range 0 to 39 with 1 column 'number'
  df.select(df.col("number") + 100) //so here we have a spark command
    .show(15) //default for show is 20

  // in Scala - moving spark operations to scala with collect
  val tinyRange = spark.range(2).toDF().collect()
  //so Collect moved the data into our own program memory because it is now an Array which is
  //which is one of our basic data Structures in Scala/Java

  val arrRow = spark.range(10).toDF(colNames = "myNumber").collect()
  //so we have an array of Rows (with a single column each here)
  //so now we can use regular Scala stuff
  arrRow.take(3).foreach(println)
  arrRow.slice(2,7).foreach(println)
  println(arrRow.last)

  import org.apache.spark.sql.types._ //this imports ALL spark datatypes
  val b = ByteType

  //TODO create a DataFrame with a single column called JulyNumbers from 1 to 31
  //TODO Show all 31 numbers

  val range1To31 = spark.range(1,32).toDF("JulyNumbers")
  range1To31.show(31)

  //TODO Create another dataframe with numbers from 100 to 3100
  //TODO show last 5 numbers

  val bigRange = spark.range(100, 3101).toDF
  bigRange.tail(5).foreach(println)

  //another approach
  val range100To3100 = spark.range(100, 3101).toDF.collect()
  range100To3100.slice(2996, 3001).foreach(println)

  val anotherDataFrame = spark.range(100,3100).toDF().collect()
  anotherDataFrame.reverse.take(5).foreach(println)

  val df100to31000 = range1To31.select(range1To31.col("JulyNumbers")*100)
  df100to31000.collect().reverse.take(5).foreach(println)



}

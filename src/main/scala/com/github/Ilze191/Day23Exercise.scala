package com.github.Ilze191

object Day23Exercise extends App {
  val spark = SparkUtil.getSpark("BasicSpark")
  //TODO is load 1st of March of 2011 into dataFrame
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  //df.show(5)

  df.createOrReplaceTempView("marchTable")
  //TODO get all purchases that have been made from Finland
  //TODO sort by Unit Price and LIMIT 20
  //TODO collect results into an Array of Rows
  //print these 20 Rows
 val finlandRows20 =  spark.sql("SELECT * FROM marchTable WHERE Country = 'Finland' ORDER BY 'UnitPrice' LIMIT 20").collect()

  for ((row, i) <- finlandRows20.zipWithIndex) {
 println(s"Row No. ${i+1} -> $row")
     }

}

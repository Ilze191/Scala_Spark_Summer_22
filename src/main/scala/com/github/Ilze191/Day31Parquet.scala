package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark

object Day31Parquet extends App{
  val spark = getSpark("Sparky")

  val df = spark.read.format("parquet")
    .load("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  df.show(5)
  df.describe().show()
  df.printSchema()

  //TODO read parquet file from src/resources/regression
  //TODO print schema
  //TODO print a sample of some rows
  //TODO show some basic statistics - describe would be a good start
  //TODO if you encounter warning reading data THEN save into src/resources/regression_fixed

  val df1 = spark.read.format("parquet")
    .load("src/resources/regression")

  df1.printSchema()
  df1.show(5)
  df1.describe().show()


}

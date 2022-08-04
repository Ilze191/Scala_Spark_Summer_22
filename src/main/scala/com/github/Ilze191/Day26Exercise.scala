package com.github.Ilze191

import com.github.Ilze191.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, desc, size, split, struct}

object Day26Exercise extends App{

  //TODO open 4th of august CSV from 2011
  //create a new dataframe with all the original columns
  //plus array of of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"

  val df = readDataWithView(spark, filePath)

  df.withColumn("Splitted", split(col("Description"), " "))
    .withColumn("Array_Length", size(col("Splitted")))
    .selectExpr("Splitted[0] as 1st", "Splitted[1] as 2nd","Splitted[2] as 3rd", "Splitted", "Array_Length")
    .where("Array_Length >= 3")
    .orderBy(desc("1st"))
    .show(10, false)

}

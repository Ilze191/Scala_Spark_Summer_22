package com.github.Ilze191

import com.github.Ilze191.SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, datediff, lit, months_between, round, to_date}

object Day25Exercise extends App{
  //TODO open March 1st , 2011
  // Add new column with current date
  // Add new column with current timestamp
  //add new column which contains days passed since InvoiceDate (here it is March 1, 2011 but it could vary)
  //add new column with months passed since InvoiceDate

  val spark = getSpark("DateFun")
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = readCSVWithView(spark, filePath)

//  val dateDF = df.withColumn("Today", current_date())
//    .withColumn("Now", current_timestamp()) //so all rows get the same timestamp
//    .withColumn("Day Difference", datediff(col("today"), col("InvoiceDate")))
//    .withColumn("Monthly Difference", months_between(col("today"), col("InvoiceDate")))
//
//  dateDF.select(col("Description"),
//    col("InvoiceDate"),
//    col("Country"),
//    col("Day Difference"),
//    col("UnitPrice"),
//    round(col("Monthly Difference"), 0)).show(5, false)
  //foodFrame.select(col("food"), col("qty")).show()
 // dateDF.show(false)
  df.select("InvoiceDate")
    .withColumn("Today", current_date())
    .withColumn("Now", current_timestamp())
    .withColumn("DayDiff", datediff(col("Today"), col("InvoiceDate")))
    .withColumn("MonthlyDiff", round(months_between(col("Today"), col("InvoiceDate"))))
    .show(10, false)



}

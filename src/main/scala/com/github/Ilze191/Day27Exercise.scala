package com.github.Ilze191
import com.github.Ilze191.SparkUtil.getSpark

import org.apache.spark.sql.functions.{col, udf}
object Day27Exercise extends App {

  //TODO create a UDF which converts Fahrenheit to Celsius
  //TODO Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want
  //TODO register your UDF function
  //TODO use your UDF to create temperatureC column with the actual conversion

  //TODO show both columns starting with F temperature at 90 and ending at 110( both included)

  val spark = getSpark("Sparky")
  val tempDF = spark.range(-40, 121).toDF("F_temp")
  def fahrenheitToC(f: Double):Double = ((f - 32) * 5 / 9).round
  val fToCUdf = udf(fahrenheitToC(_:Double):Double)

  tempDF
    .withColumn("C_temp", fToCUdf(col("F_temp")))
    .select("*")
    .where("F_temp >= 90 AND F_temp <= 110")
   // .show(21)
    .show(tempDF.count.toInt)

  //TODO simple task find count, distinct count and also aproximate distinct count (with default RSD)
  // for InvoiceNo, CustomerID AND UnitPrice columns
  //of course count should be the same for all of these because that is the number of rows





}

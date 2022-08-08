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
  spark.udf.register("Temperatures", fahrenheitToC(_:Double):Double)
  //tempDF.selectExpr("Temperatures(F_temp)").show(5)


  tempDF
    .withColumn("C_temp", fToCUdf(col("F_temp")))
    .select("*")
    .where("F_temp >= 90 AND F_temp <= 110").show(21)




}

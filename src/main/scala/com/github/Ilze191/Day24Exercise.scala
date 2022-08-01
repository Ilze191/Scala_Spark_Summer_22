package com.github.Ilze191

import com.github.Ilze191.SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, initcap, lit, lpad, regexp_replace, rpad}

object Day24Exercise extends App {

  //TODO open up March 1st, of 2011 CSV
  //Select Capitalized Description Column
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //select Description column again with all occurences of metal or wood replaced with material
  //then show top 10 results of these 3 columns
  val spark = getSpark("Working With Strings")
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = readCSVWithView(spark, filePath)

  val replacement = Seq("wood", "metal")
  val regexString = replacement.map(_.toUpperCase).mkString("|")

  val containsWood = col("Description").contains("WOOD")
  val containsMetal = col("Description").contains("METAL")

//showing 3 columns
  df.withColumn("hasMaterial", containsWood.or(containsMetal))
    .where("hasMaterial")
    .select(
   // col("Description").alias("Original description"),
    initcap(col("Description")).alias("Capitalized description"),
    lpad(rpad(col("Country"), 22, "_"), 30, "_").alias("Country"), //padding is even only for UK
    regexp_replace(col("Description"), regexString, "MATERIAL").alias("Description modifications with 'Material'")
  ).show(10,false)

  
}

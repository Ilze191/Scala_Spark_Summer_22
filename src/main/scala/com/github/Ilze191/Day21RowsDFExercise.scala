package com.github.Ilze191

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

object Day21RowsDFExercise extends App {
  val spark = SparkUtil.getSpark("BasicSpark")
  //TODO create 3 Rows with the following data formats, string - holding food name, int - for holding quantity, long for holding price
  //also boolean for holding isIt Vegan or not - so 4 data cells in each row
  // you will need to manually create a Schema - column names thus will be food, qty, price, isVegan
  // you  might need to import an extra type or two import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType }

  val schema = new StructType(Array(
    StructField("Food", StringType, false),
    StructField("Qty", IntegerType, false),
    StructField("Price", LongType, false),
    StructField("Is Vegan", BooleanType, true)))

  val foodRows = Seq(Row("Apple", 6, 1L, true),
    Row("Banana", 5, 2L, true),
    Row("Eggs", 20, 4L, false))

  //TODO create a dataFrame called foodFrame which will hold those Rows
  val myRDD1 = spark.sparkContext.parallelize(foodRows)
  val foodFrame = spark.createDataFrame(myRDD1, schema)
  //foodFrame.show()

  //Use Select or/an SQL syntax to select and show only name and qty
  val newDF = foodFrame.select("Food", "Qty")
  newDF.show()




}

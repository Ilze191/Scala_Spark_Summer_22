package com.github.Ilze191

import com.github.Ilze191.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{coalesce, col, expr, lit, to_date, to_timestamp}

object Day25DatesNulls extends App {
  println("Ch6: Dealing with Nulls in Data")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  df.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(2)

  val dateFormat = "yyyy-dd-MM"
  val euroFormat = "dd-MM-yy"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
    to_date(lit("02-08-22"), euroFormat).alias("date3"),
    to_date(lit("22_02_08"), "yy_dd_MM").alias("date4"), //does not work in Spark 3.0+ FIXME
  )
  cleanDateDF.createOrReplaceTempView("dateTable2")

  cleanDateDF.show(3,false)
  spark.sql(
    """
      |SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
      |FROM dateTable2
      |""".stripMargin)
    .show(2)

  //Now let’s use an example of to_timestamp, which always requires a format to be specified:
  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  //After we have our date or timestamp in the correct format and type, comparing between them is
  //actually quite easy. We just need to be sure to either use a date/timestamp type or specify our
  //string according to the right format of yyyy-MM-dd if we’re comparing a date
  cleanDateDF.filter(col("date3") > lit("2021-12-12")).show() //here it filters nothing

  //Coalesce
  //Spark includes a function to allow you to select the first non-null value from a set of columns by
  //using the coalesce function. In this case, there are no null values, so it simply returns the first
  //column:

  df.describe().show() //double checking for description

  df.
    withColumn("mynulls", expr("null")).
    select(coalesce(col("mynulls"), col("Description"), col("CustomerId")))
    .show()
  //so it shows the first column without nulls
  spark.sql(
    """
      |SELECT
      |ifnull(null, 'return_value'),
      |nullif('value', 'value'),
      | nvl(null, 'return_value'),
      |nvl2('not_null', 'return_value', "else_value"),
      |nvl2(null, 'return_value', "else_value")
      |FROM dfTable LIMIT 1
      |""".stripMargin).
    show(2)

  //drop
  //The simplest function is drop, which removes rows that contain nulls. The default is to drop any
  //row in which any value is null:
  println(s"Originally df is size: ${df.count()}")

  println(df.na.drop().count())
  println(df.na.drop("any").count()) //same as above drops rows where any column is null

  //all will drop rows only if ALL columns are null
  println(df.na.drop("all").count())

  //We can also apply this to certain sets of columns by passing in an array of columns:
  println("After dropping emtpy StockCode AND invoiceNo")
  println(df.na.drop("all", Seq("StockCode", "InvoiceNo")).count())
  println("Affter dropping when null Description")
  println(df.na.drop("all", Seq("Description")).count())

  // in Scala
  //so we put 777 in those cells of specific columns which are null at the moment
  df.na.fill(777, Seq("StockCode", "InvoiceNo", "CustomerID"))
    .where(expr("CustomerID = 777"))
    .show(10, false)

  //We can also do this with with a Scala Map, where the key is the column name and the value is the
  //value we would like to use to fill null values

  // in Scala
  val fillColValues = Map("CustomerID" -> 5, "Description" -> "No Description")
  df.na.fill(fillColValues).
    where(expr("Description = 'No Description'"))
    .show(5, false)

  //so with replace we can replace also not null values
  //of course we could have used our withColumn syntax to create new columns with replaced values as well

  // in Scala
  df.na.replace("Description", Map("VINTAGE SNAP CARDS" -> "BRAND NEW CARDS"))
    .where("Description = 'BRAND NEW CARDS'")
    .show(5, false)


}

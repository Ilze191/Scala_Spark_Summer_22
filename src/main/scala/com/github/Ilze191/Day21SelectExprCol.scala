package com.github.Ilze191

import org.apache.spark.sql.functions.{expr, lit}

object Day21SelectExprCol extends App {
  println("Ch 5: selectExpr and Column operations")
  val spark = SparkUtil.getSpark("Sparky")

  val flightPath = "src/resources/flight-data/json/2015-summary.json"
  //so automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.show(3)
  //  val statDf = df.describe() //shows basic statistics on string and numeric columns
  //  statDf.show()
  df.describe().show() //when you do not need to save the statistics dataframe for further use
  //so we select 3 columns
  df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME as ORIGIN").show(2)

  df.selectExpr(
    "*", // include all original columns //more useful when you only need some columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
    .show(5)

  // in Scala so same as above selectExpr
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(2)


  //how could we get the row(s) that represents withinCountry flights
  //we could check for withinCountry value being true
  //or we could do it directly

  df.where("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show()
  //should show only 1 row with US to US flights

  //With select expression, we can also specify aggregations over the entire DataFrame by taking
  //advantage of the functions that we have. These look just like what we have been showing so far
  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show()

  //Converting to Spark Types (Literals)
  //Sometimes, we need to pass explicit values into Spark that are just a value (rather than a new
  //column). This might be a constant value or something we’ll need to compare to later on. The
  //way we do this is through literals. This is basically a translation from a given programming
  //language’s literal value to one that Spark understands. Literals are expressions and you can use
  //them in the same way:

  //so before show we have created a new dataframe with an extra column with all 42s
  df.select(expr("*"), lit(42).as("The Answer!")).show(5)

  //even shorter example with selectExpr - same as above
  df.selectExpr("*", "42 as TheAnswer").show(3)

  //Adding Columns
  //There’s also a more formal way of adding a new column to a DataFrame, and that’s by using the
  //withColumn method on our DataFrame. For example, let’s add a column that just adds the
  //number one as a column

  // in Scala
  df.withColumn("number33", lit(33)).show(2)

  //we can add more than one column
  df.withColumn("numberOne", lit(1))
    .withColumn("numberTen", lit(10))
    .show(3)

  val dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))
  dfWithLongColName.show(3)

  //Here we need to use backticks because we’re
  //referencing a column in an expression
  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
    .show(2)
}

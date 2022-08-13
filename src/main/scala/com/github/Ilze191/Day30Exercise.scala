package com.github.Ilze191

import com.github.Ilze191.SparkUtil.{getSpark, readDataWithView}

object Day30Exercise extends App{
  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL

  val spark = getSpark("Sparky")

  val filePath1 = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePath2 = "src/resources/retail-data/customers.csv"
  val retailDf = readDataWithView(spark, filePath1, viewName = "retail", printSchema = false)
  val customerDf = readDataWithView(spark, filePath2, viewName = "customer", printSchema = false)

  retailDf.show(5)
  customerDf.show(5)

  val joinExpression = retailDf.col("CustomerID") === customerDf.col("Id")

  retailDf.join(customerDf, joinExpression).show()


  spark.sql(
    """
      |SELECT * FROM retail JOIN customer
      |ON retail.CustomerID = customer.id
      |""".stripMargin)
    .show()

}

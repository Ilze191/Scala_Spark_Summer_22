package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.sql.functions.{avg, col, round}

object Stock_Analysis extends App {
  val spark = getSpark("StockMarketAnalysis")

  val filePath = "src/resources/stocks/stock_prices_.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.show(5)

  // val dateFormat = "yyyy-dd-MM" //not sure yet how to drop from the date time

  println("RETURN OF EVERY STOCK EACH DAY")
  val dailyReturn = df.withColumn("daily_return", round((col("close") - col("open"))/col("open")*100,3))

  dailyReturn
    .select("date", "open", "close", "ticker", "daily_return")
    .sort("date").show(10)


  println("RETURN OF ALL STOCKS EACH DAY - AVERAGE DAILY RETURN")
  val avgDailyReturn = dailyReturn
    .groupBy("date")
    .agg(round(avg("daily_return"),3).alias("avg_daily_return"))
    .sort("date")

  avgDailyReturn.show(10)


  //TODO Save the results to the file as Parquet
//  dailyReturn.write
//    .format("parquet")
//    .mode("overwrite")
//    .save("src/resources/parquet/daily_return_per_stock.parquet")
//
//
//  avgDailyReturn.write
//    .format("parquet")
//    .mode("overwrite")
//    .save("src/resources/parquet/avg_daily_return.parquet")

  //TODO Save the results to the file as CSV
//  dailyReturn.write
//    .format("csv")
//    .mode("overwrite")
//    .option("header", true)
//    .save("src/resources/csv/daily_return.csv")
//
//  avgDailyReturn.write
//    .format("csv")
//    .mode("overwrite")
//    .option("header", true)
//    .save("src/resources/csv/avg_daily_return.csv")


// //Function for writing and saving files
//  def writingFile(dataFrame:DataFrame, path:String, format:String, header:Boolean=true): Unit = {
//    dataFrame.write
//      .format(format)
//      .mode("overwrite")
//      .option("header", header.toString)
//      .save(path)
//  }

  //Save the results to the file as SQL


}

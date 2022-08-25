package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.feature.{RFormula}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.expr

object Day36Exercise extends App {
  //TODO open "src/resources/csv/range3d"

  val spark = getSpark("Sparky")
  val src = "src/resources/csv/range3d"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(src)

  df.show(10)

  //TODO Transform x1,x2,x3 into features(you cna use VectorAssembler or RFormula), y can stay, or you can use label/value column
  val myRFormula = new RFormula()
    .setFormula("y ~ x1 + x2 + x3")
    .setLabelCol("y")
    .setFeaturesCol("features")

  val ndf = myRFormula
    .fit(df).transform(df)

  ndf.show(5)

  //TODO create  a Linear Regression model, fit it to our 3d data
  val linReg = new LinearRegression()
    .setLabelCol("y")

  val lrModel = linReg.fit(ndf)

  val summary = lrModel.summary
  summary.residuals.show(10)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0)
  val x2 = coefficients(1)
  val x3 = coefficients(2)

  //TODO print out intercept
  //TODO print out all 3 coefficients

  println(s"Intercept: $intercept | Coefficient x1: $x1 | Coefficient x3: $x2 | Coefficient x3: $x3")

//  val predictedDF = lrModel.transform(ndf)
//    .withColumn("residuals", expr("y - prediction"))
//  predictedDF.show(10, false)

  //TODO make a prediction if values or x1, x2 and x3 are respectively 100, 50, 1000
  println("Prediction if x1 = 100, x2 = 50, x3 = 100")
  println(lrModel.predict(Vectors.dense(100, 50, 1000)))

}

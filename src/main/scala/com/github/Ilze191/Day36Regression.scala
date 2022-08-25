package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.expr

object Day36Regression extends App {
  println("CH26: Regressions")

  val spark = getSpark("Sparky")
  val src = "src/resources/csv/range100"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(src) //we could also use option("path", src).load

  df.show(10)

  //now we need to once again prepare features as a vector in this case vector with single value in each row

  val rFormula = new RFormula()
    .setFormula("y ~ .") //so y is the label and rest (here just x col) are the features
    .setLabelCol("value") //default is  label
    .setFeaturesCol("features") //again features is the default already

  val ndf = rFormula
    .fit(df) //prepare the data
    .transform(df) //transform df into a new dataframe

  ndf.show(10)

  val linReg = new LinearRegression()
    .setLabelCol("value") //we could also use y

  println(linReg.explainParams()) //we can check what parameters we can adjust
  //we could also look up here
  //https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression


  val lrModel = linReg.fit(ndf) //so this already creates a model we can use for predictions

  //we can already use it to make predictions, just need to pass in Vectors type
  println(lrModel.predict(Vectors.dense(1000)))
  println(lrModel.predict(Vectors.dense(-1000)))

  val summary = lrModel.summary
  summary.residuals.show(10) //residuals are the errors, differences from actual values

  //in a linear regression we want the intercept ax+b - intercept will be b
  //coefficient(s) are the a values - so for multiple features you would have a1, a2, a3, etc
  //like 3 features would be a1x1 + a2x2 + a3x3 + b

  val intercept = lrModel.intercept //this is our b
  val coefficient = lrModel.coefficients(0) //we only have 1 features so first one (a)

  println(s"Intercept is $intercept and coefficient is $coefficient")
  //so we will find out our y = ax + b

  //of course we would also want to have a test set to check our model
  //so we use transform of our model to make prediction on some dataframe (of course we need a features column)
  val predictDF = lrModel.transform(ndf)
    .withColumn("residuals", expr("value - prediction"))

  predictDF.show(10, false)




}

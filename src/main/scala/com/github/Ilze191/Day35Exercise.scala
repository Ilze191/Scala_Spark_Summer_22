package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, expr}

object Day35Exercise extends App {

  val spark = getSpark("irisesClassification")
  val filePath = "./src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  val myRFormula = new RFormula()
    .setFormula("flower ~ . ")

  val ndf = df.withColumnRenamed("_c4", "flower")
  ndf.show(5, false)

  val fittedRF = myRFormula.fit(ndf)
  val preparedDF = fittedRF.transform(ndf)

  //TODO fit Irises data set using split of 75% for training and 25% for testing from preparedDF

  val Array(train, test) = preparedDF.randomSplit(Array(0.75, 0.25))

  //TODO use any other Classification besides Decision Tree (so LogisticRegression would be fine)


  val lr = new LogisticRegression()

  val fittedLR = lr.fit(train)
  val fittedTrain = fittedLR.transform(train)
  fittedTrain.show(5,false)

  val fittedTest = fittedLR.transform(test)

  //https://spark.apache.org/docs/3.2.2/ml-classification-regression.html
  //Check Accuracy on the testing set (you can also show accuracy on training set, but that should be 100% :) )
  def showAccuracy(df: DataFrame): Unit = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df)
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${(1.0 - accuracy)}")
  }

  showAccuracy(fittedTest)


}

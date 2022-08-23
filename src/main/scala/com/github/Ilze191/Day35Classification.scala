package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.linalg.Vectors

object Day35Classification extends App{
  val spark = getSpark("Sparky")

  val bInput = spark.read.format("parquet").load("src/resources/binary-classification")
    .selectExpr("features", "cast(label as double) as label") //casting answers as double

  bInput.show(10)
  val lr = new LogisticRegression()
    .setMaxIter(150) //how many times the algoritm will run

 // println(lr.explainParams()) // see all parameters
 lr.setMaxIter(50)
   .setElasticNetParam(0.7)
  val lrModel = lr.fit(bInput)
  println(lrModel.coefficients)
  println(lrModel.intercept)

  val myVector = Vectors.dense(1.0, 5.0, 9.6)

  println(lrModel.predict(myVector))

  val dt = new DecisionTreeClassifier()
    .setMaxDepth(5) //avoid the tree to growing too large
  //default is 5

  val dtModel = dt.fit(bInput)

  //decision tree predictor will work just like the logistic regression model
  println(dtModel.predict(myVector))







}

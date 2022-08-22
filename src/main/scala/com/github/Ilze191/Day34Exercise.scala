package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, CountVectorizer}


object Day34Exercise extends App{

  val spark = getSpark("Sparky")
  val filePath = "src/resources/text/Alice"
  //TODO using tokenized alice - from weekend exercise
  val df = spark.read.textFile(filePath).toDF("text")
  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val tokenDF = tkn.transform(df.select("text"))

  tokenDF.show(5)

  //TODO remove english stopwords
  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords)
    .setInputCol("words")
    .setOutputCol("excluded_common_words")

  val removedWordsDF = stops.transform(tokenDF)
  removedWordsDF.show(5)

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents(here that means rows)
  val cv = new CountVectorizer()
    .setInputCol("words") //
    .setOutputCol("countVec")
    .setVocabSize(1000)
    .setMinTF(1)
    .setMinDF(3)
  val fittedCV = cv.fit(tokenDF)

  //TODO show first 30 rows of data
  fittedCV.transform(tokenDF).show(30, false)


}

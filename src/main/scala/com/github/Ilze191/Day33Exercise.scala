package com.github.Ilze191

import com.github.Ilze191.SparkUtil.getSpark
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.functions.{coalesce, col, expr}

object Day33Exercise extends App{
  //TODO Read text from url
  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well
  //alternative download and read from file locally
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark
  val spark = getSpark("Sparky")
  val filePath = "src/resources/text/Alice"

  //TODO create a DataFrame with a single column called text which contains above book line by line
  val df = spark.read.textFile(filePath).toDF("text")

  //TODO create new column called words with will contain Tokenized words of text column
  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val tokenDF = tkn.transform(df.select("text"))

  tokenDF.show()

  //TODO create column called textLen which will be a character count in text column //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark
  tokenDF.withColumn("textLen", expr("CHAR_LENGTH(text)")).show()
  //TODO create column wordCount which will be a count of words in words column //can use count or length - words column will be Array type
  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features //features column will have a Vector with two of those values

  //TODO create StandardScaler which will take features column and output column called scaledFeatures //it should be using mean and variance (so both true)

  //TODO create a dataframe with all these columns - save to alice.csv file with all columns
}

name := "Scala_Spark_Summer_22"

version := "0.1"

scalaVersion := "2.13.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

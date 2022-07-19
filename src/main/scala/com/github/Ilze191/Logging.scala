package com.github.Ilze191

import org.apache.log4j.Logger

object Logging extends App {
  //logging in JVM world is done through log4j library
  println(classOf[LoggingDay18].getName)
  val log = Logger.getLogger(classOf[LoggingDay18].getName) //considered a good practice to assign classname to particular
  //so instead of println you would use this log.method
  log.debug("Hello this is a debug message")
  log.info("Hello this is an info message")
  log.warn("This is a warning")
  log.error("This is an error!")
  //there are more wrapper libraries such as Logback
  //https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
}

class LoggingDay18 //empty class just to give name to our Logger, could have used a string


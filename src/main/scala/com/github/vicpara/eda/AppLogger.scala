package com.github.vicpara.eda

import org.apache.log4j.{ Logger, PatternLayout, ConsoleAppender, Level }
import org.apache.spark.rdd.RDD

case object AppLogger {
  def getLogger(level: Level = Level.INFO): Logger = {
    val level = org.apache.log4j.Level.INFO
    val logger = org.apache.log4j.Logger.getLogger("nlp-topic-modelling")
    logger.setLevel(level)

    val capp = new ConsoleAppender()
    capp.setName(s"ConsoleAppender ${level.toString}")
    capp.setLayout(new PatternLayout("%d %m%n"))
    capp.setThreshold(level)
    capp.activateOptions()
    logger.addAppender(capp)
    logger
  }

  def logStage[T](data: RDD[T], message: String): RDD[T] = {
    infoLevel.info(message)
    data
  }

  def apply(): org.apache.log4j.Logger = infoLevel
  val infoLevel = getLogger()
}

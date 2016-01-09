package com.vicpara.eda

import org.apache.spark.{ SparkConf, SparkContext }

object StaticSparkContext {
  val staticSc = new SparkContext(
    new SparkConf().setMaster("local")
      .set("spark.ui.port", "14321")
      .set("spark.eventLog.dir", System.getProperty("java.io.tmpdir"))
      .set("spark.io.compression.codec", "lz4")
      .setAppName("Test Local Spark Context")
  )
}

trait TestUtils {
  implicit class PimpedDouble(d: Double) {
    def roundDecimal = "%.2f".format(d).toDouble
  }

  @transient lazy val sc = StaticSparkContext.staticSc
}

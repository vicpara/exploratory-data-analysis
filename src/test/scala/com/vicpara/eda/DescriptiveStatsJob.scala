package com.vicpara.eda

import com.vicpara.eda.stats.PrettyPercentileStats
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

case class Transaction(timestamp: Long, customerId: Long, businessId: Long, bPostcode: Option[String]) {
  def toSv(sep: String = "\t"): String = List(timestamp, customerId, businessId).mkString(sep)
}

case object Transaction {
  def apply(sep: String)(line: String): Transaction = line.split(sep, -1).toList match {
    case List(timestamp, customerId, businessId, postcode) => Transaction(timestamp.toLong, customerId.toLong,
      businessId.toLong, Some(postcode))
  }
}

case object DescriptiveStatsJob extends Generators {
  def main(args: Array[String]) {
    val conf = new ScallopConf(args) {
      val delim = opt[String](default = Some("\t"), descr = "The delimiter character")
      val tmpFolder = opt[String](descr = "Overrides the directory used in spark.local.dir")

      val outputPath = opt[String](required = true, descr = "The output path for the EDA stats")
      val outputPathHuman = opt[String](descr = "The output path [Human readable] for the EDA stats")
    }

    conf.printHelp()
    AppLogger().info(conf.summary)

    //    val sparkConf = new SparkConf().set("spark.akka.frameSize", "128").set("spark.hadoop.validateOutputSpecs", "false")
    //    if (conf.tmpFolder.isDefined) sparkConf.set("spark.local.dir", conf.tmpFolder())
    //    @transient val sc: SparkContext = new SparkContext(sparkConf)

    @transient val sc = StaticSparkContext.staticSc

    val transactions = sc.parallelize(randomTransactions(80000).sample.get)

    val results: RDD[PrettyPercentileStats] = sc.makeRDD(List(
      Stats.txCountPerCustomerNDayStats(transactions),
      Stats.txCountPerBusinessIdNDayStats(transactions),
      Stats.globalUniqueCustomersCounterStats(transactions),
      Stats.uniqueBusinessIdPerPostcodeNDayStats(transactions),
      Stats.uniqueCustomersPerBusinessIdNDayStats(transactions),
      Stats.uniqueCustomerIdPerPostcodeNDayStats(transactions)
    ).flatten, numSlices = 1)

    results.saveAsObjectFile(conf.outputPath())
    conf.outputPathHuman.foreach(results.map(_.toHumanReadable).saveAsTextFile)
  }
}

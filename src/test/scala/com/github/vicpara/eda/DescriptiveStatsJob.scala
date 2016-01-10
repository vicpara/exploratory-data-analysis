package com.github.vicpara.eda

import com.github.vicpara.eda.stats.PrettyPercentileStats
import io.continuum.bokeh.{ Document, Plot, GridPlot }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.rogach.scallop.ScallopConf

case class Transaction(timestamp: Long, customerId: Long, businessId: Long, postcode: Option[String]) {
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

    val sparkConf = new SparkConf()
      .set("spark.akka.frameSize", "128")
      .setMaster("local")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.io.compression.codec", "lz4")
      .setAppName("Local Exploratory Data Analysis")

    if (conf.tmpFolder.isDefined) sparkConf.set("spark.local.dir", conf.tmpFolder())
    @transient val sc: SparkContext = new SparkContext(sparkConf)

    val transactions = sc.parallelize(randomTransactions(80000).sample.get)
    AppLogger.logStage(transactions, "Finished generating the data points")

    val results: RDD[PrettyPercentileStats] = sc.makeRDD(List(
      Stats.txCountPerCustomerNDayStats(transactions, 101),
      Stats.txCountPerBusinessIdNDayStats(transactions, 101),

      Stats.uniqueBusinessIdPerPostcodeNDayStats(transactions, 101),
      Stats.uniqueCustomersPerBusinessIdNDayStats(transactions, 101),
      Stats.uniqueCustomerIdPerPostcodeNDayStats(transactions, 101),

      Stats.globalUniqueCustomersCounterStats(transactions, 101),
      Stats.globalUniqueBusinessesCounterStats(transactions, 101),
      Stats.globalUniquePostcodesCounterStats(transactions, 101)
    ).flatten, numSlices = 1)

    results.saveAsObjectFile(conf.outputPath())
    conf.outputPathHuman.foreach(results.map(_.toHumanReadable).saveAsTextFile)

    savePlotLists(results, "/tmp/eda/human/")
  }

  def savePlotLists(results: RDD[PrettyPercentileStats], outFolder: String) = {
    val plotGrid: List[List[Plot]] = results.collect()
      .flatMap(_.toPlot)
      .zipWithIndex
      .groupBy(_._2 / 2)
      .map(_._2.map(_._1).toList).toList

    val grid = new GridPlot().children(plotGrid)

    val document = new Document(grid)
    val html = document.save(outFolder + "/" + "EDA_Stats_Results.html")
    AppLogger.infoLevel.info(s"Wrote EDA stats charts in ${html.file}. Open ${html.url} in a web browser.")
  }
}

package com.vicpara.eda.stats

import scalaz.Scalaz._

case class PercentileStats(points: List[(String, Long)], numBuckets: Long)

case class PercentileStatsWithFilterLevel(drillDownFilterValue: String, stats: PercentileStats)

case class PrettyPercentileStats(name: String, levels: List[PercentileStatsWithFilterLevel]) {

  def toHumanReadable: String = levels.map(l => s"\n${"_" * 148}\n$name \tDrillDownValue : ${l.drillDownFilterValue}\n\t" +
    s"${l.stats |> prettyContent}").mkString("\n")

  def prettyContent(stats: PercentileStats): String = stats.points match {
    case points if points.isEmpty => "[EmptyStatistic]"
    case points if points.size == 1 => s"[SingleStats]: Value: ${stats.points.head._2}\n"

    case points if points.size > 1 =>
      val min +: _ :+ max = stats.points.map(_._2)

      val pointsPerChar: Double = (max - min) / 100.0
      val fillChar = "#"

      val histPretty: String =
        stats.points.zipWithIndex.map {
          case ((key, value), index) =>
            val strIndex = "%5s".format(index)
            val label = "%35s".format(key)
            val bars = "%101s".format(fillChar * (1 + ((value - min) / pointsPerChar).toInt)).reverse
            s"$label |$strIndex| $bars| $value"
        }
        .mkString("\n")
      s"[PercentileStats]: NumBinsInHistogram: ${stats.numBuckets.toString}\n$histPretty"
  }
}

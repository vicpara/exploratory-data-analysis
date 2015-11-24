package com.vicpara.eda.stats

import breeze.numerics.sin
import com.vicpara.eda.AppLogger
import io.continuum.bokeh._
import math.{Pi => pi}

import scalaz.Scalaz._

case class PercentileStats(points: List[(String, Long)], numBuckets: Long)

case class PercentileStatsWithFilterLevel(drillDownFilterValue: String, stats: PercentileStats)

case class PrettyPercentileStats(name: String, levels: List[PercentileStatsWithFilterLevel]) {
  def lineImage(xvals: Seq[Double], yvals: Seq[Double], name: String): Plot = {
    object source extends ColumnDataSource {
      val x = column(xvals)
      val y = column(yvals)
    }

    import source.{x, y}

    val xdr = new DataRange1d()
    val ydr = new DataRange1d()

    val line = new Line().x(x).y(y).line_color(Color.BlueViolet)

    val renderer = new GlyphRenderer()
                   .data_source(source)
                   .glyph(line)

    val plot = new Plot().title(name).x_range(xdr).y_range(ydr)

    val xaxis = new LinearAxis().plot(plot)
    val yaxis = new LinearAxis().plot(plot)
    plot.below <<= (xaxis :: _)
    plot.left <<= (yaxis :: _)

    val pantool = new PanTool().plot(plot)
    val wheelzoomtool = new WheelZoomTool().plot(plot)

    plot.renderers := List(xaxis, yaxis, renderer)
    plot.tools := List(pantool, wheelzoomtool)

    plot
    //    val document = new Document(plot)
    //    val fileOut = folder + "/" + name + ".line.html"
    //    val html = document.save(fileOut)
    //
    //    AppLogger.infoLevel.info(s"Wrote ${html.file}. Open ${html.url} in a web browser.")
  }

  def toChart: List[Plot] =
    levels.flatMap(stats => if (stats.stats.points.size > 1)
      Some(lineImage(xvals = stats.stats.points.indices.map(_.toDouble),
        yvals = stats.stats.points.map(_._2.toDouble),
        name = name + stats.drillDownFilterValue))
    else None)

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

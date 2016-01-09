package com.vicpara.eda.stats

import com.vicpara.eda.AppLogger
import io.continuum.bokeh._

import scalaz.Scalaz._

case class PercentileStats(points: List[(String, Long)], numBuckets: Long) {
  def isSingleValue = points match {
    case pts if pts.size == 1 => true
    case _ => false
  }
}

case class PercentileStatsWithFilterLevel(drillDownFilterValue: String, stats: PercentileStats) {
  def isSingleStat = drillDownFilterValue.equals(SequenceStats.drillDownKeyAll)
  def isSingleValue = stats.points.size == 1
}

case class PrettyPercentileStats(name: String, levels: List[PercentileStatsWithFilterLevel]) {
  def lineImage(xvals: Seq[Double], yvals: Seq[Double], name: String): Plot = {
    object source extends ColumnDataSource {
      val x = column(xvals)
      val y = column(yvals)
    }

    import source.{ x, y }

    val xdr = new DataRange1d()
    val ydr = new DataRange1d()

    val line = new Line().x(x).y(y).line_color("#666699").line_width(2)

    val line_renderer = new GlyphRenderer()
      .data_source(source)
      .glyph(line)

    val plot = new Plot()
      .title(name).title_text_font_size(FontSize.apply(10, FontUnits.PT))
      .x_range(xdr).y_range(ydr)
      .width(500).height(500)
      .border_fill(Color.White)
      .background_fill("#FFE8C7")

    val xaxis = new LinearAxis().plot(plot)
    val yaxis = new LinearAxis().plot(plot)
    plot.below <<= (xaxis :: _)
    plot.left <<= (yaxis :: _)
    val xgrid = new Grid().plot(plot).axis(xaxis).dimension(0)
    val ygrid = new Grid().plot(plot).axis(yaxis).dimension(1)

    val pantool = new PanTool().plot(plot)
    val wheelzoomtool = new WheelZoomTool().plot(plot)

    plot.tools := List(pantool, wheelzoomtool)
    plot.renderers := List(xaxis, yaxis, xgrid, ygrid, line_renderer)

    plot
  }

  def toPlot: List[Plot] =
    levels.flatMap(stats => if (stats.stats.points.size > 1)
      Some(lineImage(
      xvals = stats.stats.points.indices.map(_.toDouble),
      yvals = stats.stats.points.map(_._2.toDouble),
      name = name + " #" + stats.drillDownFilterValue
    ))
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

      val maxLabelLength = math.min(100, stats.points.map(_._1.length).max) + 5

      val histPretty: String =
        stats.points
          .zipWithIndex.map {
            case ((key, value), index) =>
              val strIndex = "%5s".format(index)
              val label = s"%${maxLabelLength}s".format(key)
              val bars = "%101s".format(fillChar * (1 + ((value - min) / pointsPerChar).toInt)).reverse
              s"$label |$strIndex| $bars| $value"
          }
          .mkString("\n")

      s"[PercentileStats]: NumBinsInHistogram: ${stats.numBuckets.toString}\n$histPretty"
  }
}

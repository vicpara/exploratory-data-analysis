package com.vicpara.eda.stats

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scalaz.Scalaz._

case object SequenceStats {
  val drillDownKeyAll = "DrillDownKey.ALL"

  def percentile[T: ClassTag, DimKey: ClassTag, V: ClassTag](data: RDD[T],
                                                             toDrillDownKeyOption: Option[T => String],
                                                             toDimKey: T => DimKey,
                                                             toVal: T => V,
                                                             toStats: V => Long,
                                                             reduceFunc: (V, V) => V,
                                                             numPercentiles: Int): List[PercentileStatsWithFilterLevel] =

    data.map(t => (toDrillDownKeyOption.getOrElse((_: T) => drillDownKeyAll)(t), toDimKey(t)) -> toVal(t))
    .reduceByKey(reduceFunc)
    .mapValues(toStats)
    .sortBy(_._2)
    .zipWithIndex() |>
      percentileStats(numPercentiles) |>
      (percentileStats => percentileStats.toList.map((PercentileStatsWithFilterLevel.apply _).tupled))

  def distinct[T: ClassTag, DimKey: ClassTag, V: ClassTag](data: RDD[T],
                                                           toDrillDownKeyOption: Option[T => String],
                                                           toDimKey: T => DimKey,
                                                           toVal: T => V,
                                                           numPercentiles: Int): List[PercentileStatsWithFilterLevel] =
    data.map(t => (toDrillDownKeyOption.getOrElse((_: T) => drillDownKeyAll)(t), toDimKey(t), toVal(t)))
    .distinct()
    .map(e => (e._1, e._2) -> Set(e._3))
    .reduceByKey(_ ++ _)
    .mapValues(_.size.toLong)
    .sortBy(_._2)
    .zipWithIndex() |>
      percentileStats(numPercentiles) |>
      (percentileStats => percentileStats.toList.map((PercentileStatsWithFilterLevel.apply _).tupled))

  def percentileStats[DimKey](numPercentiles: Int = 1001)
                             (data: RDD[(((String, DimKey), Long), Long)]): Map[String, PercentileStats] = {

    val numBuckets: Map[String, Int] =
      data.map {
        case (((drillDownKey, _), _), _) => drillDownKey -> 1
      }
      .reduceByKey(_ + _)
      .collect()
      .toMap

    val drillDownKeyToIndices: Map[String, Set[Long]] =
      numBuckets.mapValues(percentileIndices(_, numPercentiles)).map(identity)

    data.filter { case (((drillDownKey, _), _), index) => drillDownKeyToIndices(drillDownKey).contains(index)}
    .map {
      case (((drillDownKey, dimKey), stats), _) => drillDownKey -> (dimKey.toString -> stats)
    }
    .collect()
    .groupBy(_._1)
    .mapValues(_.map(_._2).toList.sortBy(_._2))
    .map {
      case (key, points) => key -> PercentileStats(points, numBuckets(key))
    }
  }

  def percentileIndices(numElems: Long, numPercentiles: Int): Set[Long] = {
    val delta: Double = (1.0 / math.max(1, numPercentiles - 1)) * (numElems - 1)
    (0 until numPercentiles).map(el => (el * delta).round).toSet
  }
}

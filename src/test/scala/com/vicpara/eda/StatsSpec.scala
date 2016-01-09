package com.vicpara.eda

import com.vicpara.eda.stats.{ SequenceStats, PercentileStats, PercentileStatsWithFilterLevel, PrettyPercentileStats }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.specs2.matcher.ScalaCheckMatchers
import org.specs2.mutable.Specification

import scalaz.Scalaz._

case object StatsSpec extends Specification with ScalaCheckMatchers with TestUtils {
  "ExploratoryDataAnalysis" should {

    "correctly compute percentiles statistics on monotonic increasing 21 sample dataset" in {
      val rawData =
        (0l until 21).map((" ", _))
          .map {
            case (drillDownKey, dimKey) => (((drillDownKey, dimKey), dimKey), dimKey)
          }
          .toList
      val dataRDD: RDD[(((String, Long), Long), Long)] = sc.parallelize(rawData)

      val res: PercentileStats = SequenceStats.percentileStats[Long](numPercentiles = 21)(dataRDD).head._2
      res must_=== PercentileStats(points = rawData.map(el => (el._2.toString, el._2)), numBuckets = 21l)
    }

    "correctly compute the right number of elements in the percentiles statistics on 101 constant samples dataset" in {
      val key = " "
      val rawData: List[(((String, Long), Long), Long)] = (0l until 101 by 1).toList.map(e => (((key, e), e), e))
      val dataRDD: RDD[(((String, Long), Long), Long)] = sc.parallelize(rawData)

      val intRes = SequenceStats.percentileStats[Long](numPercentiles = 11)(dataRDD)
      val res =
        List(1, 2, 3, 5, 6, 7, 9, 11, 20, 21, 99, 100)
          .map(r => (r, SequenceStats.percentileStats[Long](numPercentiles = r)(dataRDD).get(key).get.points.size))
          .filter(e => e._1 != e._2)

      res.foreach(el => el._1 must_=== el._2)
      res.size must_=== 0
    }

    "correctly compute number percentiles when Num Percentiles is larger than samples" in {
      val numSamplesList = List(2, 4, 5, 10, 11, 13, 17, 21, 39, 55, 101)
      val res = numSamplesList.flatMap(numSample => {
        val ires =
          List(102, 103, 104, 101, 130, 200, 300, 500, 1000)
            .map(v => (numSample, SequenceStats.percentileIndices(numSample, v).size))
            .filter(v => v._1 != v._2)

        ires.forall(el => el._1 must_=== el._2)
        ires.size must_=== 0
        ires
      })
      if (res.nonEmpty)
        println("Numer of percentile indices" + res.map(el => s"Expected:${el._1} but got ${el._2} ").mkString("\n"))
      res.size must_=== 0
    }

    "correctly compute percentiles statistics on 101 constant samples dataset" in {
      val rawData: List[(((String, Long), Long), Long)] = (0l until 101 by 1).toList.map(e => (((" ", e), 2l), e))
      val dataRDD: RDD[(((String, Long), Long), Long)] = sc.parallelize(rawData)

      val result = SequenceStats.percentileStats[Long](numPercentiles = 11)(dataRDD).head._2
      val expected = PercentileStats(points = (0 until 101 by 10).toList.zipWithIndex.map(e => (e._1.toString, 2l)), numBuckets = 101)
      result must_=== expected
    }

    "correctly compute percentiles statistics on smaller dataset than stats" in {
      val rawData = (1l until 6 by 1).map(e => (((" ", e), e), e - 1))
      val dataRDD: RDD[(((String, Long), Long), Long)] = sc.parallelize(rawData)

      val result: PercentileStats = SequenceStats.percentileStats[Long](numPercentiles = 10)(dataRDD).head._2
      val expected = PercentileStats(points = (1l until 6 by 1).map(e => (e.toString, e)).toList, numBuckets = 5l)

      result must_=== expected
    }

    "correctly computes the number of Percentile Indices for increasing number of percentiles" in {
      val res =
        List(1, 2, 3, 5, 6, 7, 9, 11, 17, 20, 21, 40, 50, 51, 99, 100, 101)
          .map(v => (v, SequenceStats.percentileIndices(101, v).size))
          .filter(v => v._1 != v._2)

      res.foreach(el => el._1 must_=== el._2)
      res.size must_=== 0
    }

    "correctly generate 10 Percentile Indexes from 1 to 10" in {
      val result: Set[Long] = SequenceStats.percentileIndices(10, 10)
      val expected: Set[Long] = (0 until 10).toList.map(_.toLong).toSet

      result must_=== expected
    }

    "correctly generate 10 Percentile Indexes from 1 to 10 when requested 20 for a smaller dataset" in {
      val result: Set[Long] = SequenceStats.percentileIndices(10, 20)
      val expected: Set[Long] = (0 until 10).toList.map(_.toLong).toSet

      result must_=== expected
    }

    "correctly generate 10 Percentile Indexes from 0 to 1000 by 100 when requested 10 for a 1001 dataset" in {
      val result: Set[Long] = SequenceStats.percentileIndices(1001, 11)
      val expected: Set[Long] = (0 until 1001 by 100).toList.map(_.toLong).toSet

      result must_=== expected
    }

    "correctly pretty prints humanReadable" in {
      val setEmpty = PrettyPercentileStats(name = "xxx",
        levels = List(PercentileStatsWithFilterLevel("NONE",
          stats = PercentileStats(points = Nil, numBuckets = 0))))

      val set1 = PrettyPercentileStats(name = "xxx",
        levels = List(PercentileStatsWithFilterLevel("NONE",
          stats = PercentileStats(points = List(("Key1", 2l)), numBuckets = 1))))

      val set2 = PrettyPercentileStats(name = "xxx",
        levels = List(PercentileStatsWithFilterLevel("NONE",
          stats = PercentileStats(points = List(("Key1", 1l), ("Key2", 2l)), numBuckets = 2))))

      val set3 = PrettyPercentileStats(name = "xxx",
        levels = List(PercentileStatsWithFilterLevel("NONE",
          stats = PercentileStats(points = List(("Key1", 1l), ("Key2", 2l), ("Key3", 3l)), numBuckets = 3))))

      List(setEmpty, set1, set2, set3)
        .map(e => e.levels.head.stats.points.size -> e.toHumanReadable)
        .map(e => e._1 -> (e._1 == e._2.split("\n").size + 2))
        .count(_._2) must_=== 0
    }

    "correctly pretty prints for humans bad samples" in {
      val data = List(
        PrettyPercentileStats(name = "BusinessId x Day - Count(Tx)",
          levels = List(PercentileStatsWithFilterLevel(drillDownFilterValue = "DrillDownKey.ALL",
            stats = PercentileStats(points = List(("1", 1830683l)), numBuckets = 1l)))),

        PrettyPercentileStats(name = "Postcode x Day - Count(RichTx)",
          levels = List(PercentileStatsWithFilterLevel(drillDownFilterValue = "DrillDownKey.ALL",
            PercentileStats(points = List((("YO126EE", "2014-12-02").toString(), 1l),
              (("CH441BA", "2014-09-23").toString(), 1l), (("LS287BJ", "2014-10-24").toString(), 1l),
              (("G156RX", "2014-01-08").toString(), 1l)), numBuckets = 4))))
      )

      val hr = data.map(_.toHumanReadable)
      hr.foreach(println)
      hr.nonEmpty must_=== true
    }

    "correctly runs end to end SequenceStats.percentile on constant dataset" in {
      case class DataP(k: String, v: Int)
      val dataRDD = sc.parallelize((0 until 101).map(el => DataP(k = el.toString, v = el)))

      @transient implicit lazy val isc: SparkContext = sc

      val res = SequenceStats.percentile[DataP, String, Long](data = dataRDD,
        toDrillDownKeyOption = None,
        toDimKey = _.k,
        toVal = _ => 1l,
        toStats = identity,
        reduceFunc = _ |+| _,
        numPercentiles = 10)

      println(PrettyPercentileStats(name = "Constant Dataset", levels = res).toHumanReadable)

      val expected = PercentileStats(points = (0 until 10).toList.map(e => (e.toString, 1l)), numBuckets = 101l)
      res.head.stats.points.map(_._2) must_=== expected.points.map(_._2)
    }

    "correctly runs end to end SequenceStats.percentile on increasing 10 bucket dataset" in {
      case class DataP(k: String, v: Int)
      val dataRDD = sc.parallelize((1 until 11).flatMap(el => (0 until el).map(num => DataP(k = el.toString, v = 1))))

      @transient implicit lazy val isc: SparkContext = sc

      val res = SequenceStats.percentile[DataP, String, Long](data = dataRDD,
        toDrillDownKeyOption = None,
        toDimKey = _.k,
        toVal = _ => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = 10)

      println(PrettyPercentileStats(name = "", levels = res).toHumanReadable)

      val expected = PercentileStats(points = (1 until 11).toList.map(e => (e.toString, e.toLong)),
        numBuckets = 10l)

      res.head.stats must_=== expected
      res.head.stats.points.map(_._2) must_=== expected.points.map(_._2)
    }
  }
}

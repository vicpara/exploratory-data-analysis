package com.vicpara.eda

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import com.vicpara.eda.stats.{PrettyPercentileStats, SequenceStats}

case object Stats {
  def txCountPerCustomerNDayStats(transactions: RDD[Transaction]) =
    Some("Customer x Day - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = tx => (tx.customerId, dayAsString(tx.timestamp)),
        toVal = _ => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = 1001
      )
    ))

  def txCountPerBusinessIdNDayStats(transactions: RDD[Transaction]) =
    Some("BusinessId x Day - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = tx => (tx.businessId, dayAsString(tx.timestamp)),
        toVal = r => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = 1001)
    ))

  def txCountPerSectorNDayStats(richTx: RDD[RichTx]) =
    Some("Postcode x Day - Count(RichTx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[RichTx, (String, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (postcodeToGranularity(t.bPostcode, SectorGranularity), dayAsString(t.timestamp)),
        toVal = r => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = 1001
      )
    ))

  def txCountPerDistrictNDayStats(richTx: RDD[RichTx]) =
    Some("Postcode x Day - Count(RichTx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[RichTx, (String, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (postcodeToGranularity(t.bPostcode, DistrictGranularity), dayAsString(t.timestamp)),
        toVal = r => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = 1001
      )
    ))

  def globalUniqueCustomersCounterStats(transactions: RDD[Transaction]) =
    Some("Global distinct Customers")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, Long, Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => 1l,
        toVal = tx => tx.customerId,
        numPercentiles = 1001
      )
    ))

  def uniqueCustomerIdPerPostcodeNDayStats(richTx: RDD[RichTx]) =
    Some("Postcode x Day - Distinct(CustomerID)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[RichTx, (String, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (t.bPostcode.getOrElse(""), dayAsString(t.timestamp)),
        toVal = tx => tx.customerId,
        numPercentiles = 1001
      )
    ))

  def uniqueCustomersPerBusinessIdNDayStats(richTx: RDD[RichTx]) =
    Some("BusinessId x Day - Distinct(CustomerId)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[RichTx, (Long, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = rtx => (rtx.businessId, dayAsString(rtx.timestamp)),
        toVal = rtx => rtx.customerId,
        numPercentiles = 1001
      )
    ))

  def uniqueBusinessIdPerPostcodeNDayStats(richTx: RDD[RichTx]) =
    Some("Postcode x Day - Distinct(BusinessId)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[RichTx, (String, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (t.bPostcode.getOrElse(""), dayAsString(t.timestamp)),
        toVal = tx => tx.businessId,
        numPercentiles = 1001
      )
    ))

  def postcodeToGranularity(postcode: Option[String], granularity: PostcodeGranularity, default: String = ""): String =
    postcode.flatMap(p => Some(
      GranularPostcode(p, UnitGranularity)
      .bin(granularity)
      .postcodePrefix
    ))
    .getOrElse(default)

  def dayAsString(millis: Long): String = new DateTime(millis).toString("yyyy-MM-dd")
}

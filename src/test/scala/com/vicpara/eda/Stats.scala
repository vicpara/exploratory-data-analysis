package com.vicpara.eda

import com.vicpara.eda.stats.{SequenceStats, PrettyPercentileStats}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case object Stats {
  def txCountPerCustomerNDayStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Customer x Day - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = tx => (tx.customerId, dayAsString(tx.timestamp)),
        toVal = _ => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = nPercentiles
      )
    ))

  def txCountPerBusinessIdNDayStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("BusinessId x Day - Count(Tx)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = tx => (tx.businessId, dayAsString(tx.timestamp)),
        toVal = r => 1l,
        toStats = identity,
        reduceFunc = _ + _,
        numPercentiles = nPercentiles)
    ))

  def globalUniqueCustomersCounterStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Global distinct Customers")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, Long, Long](data = transactions.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => 1l,
        toVal = tx => tx.customerId,
        numPercentiles = nPercentiles
      )
    ))

  def uniqueCustomerIdPerPostcodeNDayStats(richTx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Postcode x Day - Distinct(CustomerID)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, (String, String), Long](
        data = richTx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (t.bPostcode.getOrElse(""), dayAsString(t.timestamp)),
        toVal = tx => tx.customerId,
        numPercentiles = nPercentiles
      )
    ))

  def uniqueBusinessIdPerPostcodeNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Postcode x Day - Distinct(BusinessId)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, (String, String), Long](
        data = tx.setName(n),
        toDrillDownKeyOption = None,
        toDimKey = t => (t.bPostcode.getOrElse(""), dayAsString(t.timestamp)),
        toVal = tx => tx.businessId,
        numPercentiles = nPercentiles
      )
    ))

  def uniqueCustomersPerBusinessIdNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("BusinessId x Day - Distinct(CustomerId)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, (Long, String), Long](
        data = tx.setName(n),
        toDrillDownKeyOption = Some(tx => dayOfWeek(tx.timestamp)),
        toDimKey = tx => (tx.businessId, dayAsString(tx.timestamp)),
        toVal = tx => tx.customerId,
        numPercentiles = nPercentiles
      )
    ))

  def dayAsString(millis: Long): String = new DateTime(millis).toString("yyyy-MM-dd")
  def dayOfWeek(millis: Long): String = new DateTime(millis).dayOfWeek().getAsString
}

package com.vicpara.eda

import com.vicpara.eda.stats.{ SequenceStats, PrettyPercentileStats }
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case object Stats {
  def txCountPerCustomerNDayStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Customer x Day - Count(Tx)")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = AppLogger.logStage(transactions, n),
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
        levels = SequenceStats.percentile[Transaction, (Long, String), Long](data = AppLogger.logStage(transactions, n),
          toDrillDownKeyOption = None,
          toDimKey = tx => (tx.businessId, dayAsString(tx.timestamp)),
          toVal = r => 1l,
          toStats = identity,
          reduceFunc = _ + _,
          numPercentiles = nPercentiles)
      ))

  def globalUniqueBusinessesCounterStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Global distinct Businesses")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, Long, Long](data = AppLogger.logStage(transactions, n),
          toDrillDownKeyOption = None,
          toDimKey = t => 1l,
          toVal = tx => tx.businessId,
          numPercentiles = nPercentiles
        )
      ))

  def globalUniquePostcodesCounterStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Global distinct Postcodes")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, Long, String](data = AppLogger.logStage(transactions, n),
          toDrillDownKeyOption = None,
          toDimKey = t => 1l,
          toVal = tx => tx.postcode.get,
          numPercentiles = nPercentiles
        )
      ))

  def globalUniqueCustomersCounterStats(transactions: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Global distinct Customers")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, Long, Long](data = AppLogger.logStage(transactions, n),
          toDrillDownKeyOption = None,
          toDimKey = t => 1l,
          toVal = tx => tx.customerId,
          numPercentiles = nPercentiles
        )
      ))

  def uniqueCustomerIdPerPostcodeNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Postcode x Day - Distinct(CustomerID)")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, (String, String), Long](
          data = AppLogger.logStage(tx, n),
          toDrillDownKeyOption = None,
          toDimKey = t => (t.postcode.getOrElse(""), dayAsString(t.timestamp)),
          toVal = tx => tx.customerId,
          numPercentiles = nPercentiles
        )
      ))

  def uniqueBusinessIdPerPostcodeNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Postcode x Day - Distinct(BusinessId)")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, (String, String), Long](
          data = AppLogger.logStage(tx, n),
          toDrillDownKeyOption = None,
          toDimKey = t => (t.postcode.getOrElse(""), dayAsString(t.timestamp)),
          toVal = tx => tx.businessId,
          numPercentiles = nPercentiles
        )
      ))

  def uniqueCustomersPerBusinessIdNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("BusinessId x Day - Distinct(CustomerId)")
      .map(n => PrettyPercentileStats(
        name = n,
        levels = SequenceStats.distinct[Transaction, (Long, String), Long](
          data = AppLogger.logStage(tx, n),
          toDrillDownKeyOption = Some(tx => dayOfWeek(tx.timestamp)),
          toDimKey = tx => (tx.businessId, dayAsString(tx.timestamp)),
          toVal = tx => tx.customerId,
          numPercentiles = nPercentiles
        )
      ))

  def dayAsString(millis: Long): String = new DateTime(millis).toString("yyyy-MM-dd")
  def dayOfWeek(millis: Long): String = new DateTime(millis).dayOfWeek().getAsText
}

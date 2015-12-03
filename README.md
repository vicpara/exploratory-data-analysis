# Exploratory-Data-Analysis - EDA  [![Build Status](https://travis-ci.org/vicpara/exploratory-data-analysis.svg?branch=master)](https://travis-ci.org/vicpara/exploratory-data-analysis)
Spark library for doing exploratory data analysis on your data set in a scalable way.

-----
### The problem

Before running any algorithm on a data set it is essential to understand how the data looks like and what are the expected edge cases.
Maybe you have some expectations about the distribution of the numbers. Are all the buckets that you expect to find in the dataset populated with numbers?
Very often a data set is provided which contains two types of columns::

* dimensions
* measurements

Dimensions are columns that help describe the data points such as: Date, UniqueID, Gender, Postcode, Country, Categories.
Measurements are metrics that quantitatively characterize the described datum.

Depending on your analytical task, some sort of ordering or bucketing will be used in your model. Some examples::

* process daily transactions of a customer
* aggregate daily transactions of a merchant
* count the number of unique customers per merchant or postcode, sector or district
* number of transactions per day.

Let's take as example the metric : number of daily transactions per merchant

This is made out of two concepts. A key which is (dayId, merchantId) and a value count(transactions).
Depending on the richness of your dataset is there may be keys with high number of transactiona and keys with very small number. This becomes a problem especially when you are dealing with a sample dataset which promises to capture the entire diversity of the original, complete dataset.

-----
###the library

For a collection / RDD[T] given a ::

* function that maps an element of T to a *key*
* function that maps an element of T to a *value* that corresponds to the *key*
* function to aggregate in a monoid-al fashion two *values*


It can compute two types of statistics::

* the percentile values of the ordered sequence key-statistic
* the unique count of specific key


The results are saved into 2 possible formats:

* object file
* pretty text format ready for inspection

Example of the *pretty* output
```
BusinessId x Day - Distinct(CustomerId) 	DrillDownValue : Tuesday
	[PercentileStats]: NumBinsInHistogram: 1188
          (9191,2015-10-27) |    0| #######                                                | 44
          (6305,2015-11-10) |    1| ######################                                 | 51
          (6774,2015-11-03) |    2| ###########################                            | 53
          (4278,2015-11-03) |    3| #################################                      | 54
          (9191,2015-11-03) |    4| #################################                      | 54
          (4687,2015-11-17) |    5| ###################################                    | 55
          (380,2015-11-03)  |    6| ######################################                 | 56
          (8114,2015-11-03) |    7| ############################################           | 57
          (5629,2015-10-27) |    8| ############################################           | 57
          (404,2015-11-03)  |    9| ###############################################        | 58
          (7586,2015-11-10) |   10| ###############################################        | 58
          (3765,2015-11-10) |   11| ###############################################        | 58
          (8478,2015-11-17) |   12| ###############################################        | 58
          (3701,2015-10-27) |   13| ###################################################### | 60
          
```

#### Example distinct statistic
For a datum of type transaction 
```scala

case class Transaction(timestamp: Long, customerId: Long, businessId: Long, postcode: Option[String]) {
  def toSv(sep: String = "\t"): String = List(timestamp, customerId, businessId).mkString(sep)
}

```

we define a distinct statistic over number of unique businesses (distinct businessId) inside a *postcode* for a given *day* for each unique values of the key *toDrillDownKeyOption*. The value of this field would normally be a categorical segmentation axis which is expected to have relatively small cardinality. Examples of fields that could be used for toDrillDownKeyOption would be country / city / type of product / region.

```scala

  def uniqueBusinessIdPerPostcodeNDayStats(tx: RDD[Transaction], nPercentiles: Int = 1001) =
    Some("Postcode x Day - Distinct(BusinessId)")
    .map(n => PrettyPercentileStats(
      name = n,
      levels = SequenceStats.distinct[Transaction, (String, String), Long](
        data = tx,
        toDrillDownKeyOption = None,
        toDimKey = t => (t.postcode.getOrElse(""), dayAsString(t.timestamp)),
        toVal = tx => tx.businessId,
        numPercentiles = nPercentiles
      )
    ))
    
```


#### Example Percentile statistic
```scala

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
    
```

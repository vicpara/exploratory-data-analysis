package com.vicpara.eda

import org.joda.time.{ DateTime, DateTimeZone, Interval, LocalDateTime }
import org.scalacheck.{ Prop, Arbitrary, Gen }

import scala.collection.immutable
import scalaz.Scalaz._

trait Generators {
  val nTransactions = 1000000
  val nCustomers = 800
  val nBusinesses = 300

  val nonNegativeLongGen: Gen[Long] = Gen.choose[Long](0l, Long.MaxValue - 1)
  val nonNegativeIntGen: Gen[Int] = Gen.choose(0, 100)
  val positiveIntGen: Gen[Int] = Gen.choose(1, 100)

  val nonEmptyAlphaStr = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString).suchThat(_.forall(_.isLetter))

  val noOfDays = 23
  val dates = DateTime.now() |> (t0 => (0 until noOfDays).map(t0.minusDays))

  val availablePostcodes = Gen.listOfN(100, Gen.listOfN(6, Gen.alphaNumChar).flatMap(_.mkString("").toUpperCase()))
    .sample.get

  val businesses: List[Int] = Gen.listOfN(nBusinesses, Gen.choose(0, 10000)).sample.get
  val customers: List[Int] = Gen.listOfN(nCustomers, Gen.chooseNum(10001, 20000)).sample.get

  def sameDayTimestampGen(date: DateTime): Gen[DateTime] = for {
    hourOfDay <- Gen.choose(0, 23)
    minuteOfHour <- Gen.choose(0, 59)
  } yield {
    val dtz = DateTimeZone.forID("Europe/London")
    val ldt = new LocalDateTime(date.getMillis, dtz).withTime(hourOfDay, minuteOfHour, 0, 0)
    (if (dtz.isLocalDateTimeGap(ldt)) ldt.plusHours(2) else ldt).toDateTime(dtz)
  }

  def timestampGen(interval: Interval) = for {
    date: DateTime <- Gen.choose(interval.getStart.getMillis, interval.getEnd.getMillis).map(new DateTime(_))
    timestamp <- sameDayTimestampGen(date)
  } yield timestamp

  val timestampGen: Gen[DateTime] = for {
    date: DateTime <- Gen.oneOf(dates)
    timestamp <- sameDayTimestampGen(date)
  } yield timestamp

  val todayTimestampGen: Gen[DateTime] = for {
    hourOfDay <- Gen.choose(0, 23)
    minuteOfHour <- Gen.choose(0, 59)
    date = DateTime.now()
    timestamp = date.withTime(hourOfDay, minuteOfHour, 0, 0)
  } yield timestamp

  val postcodeGen: Gen[String] = Gen.oneOf(availablePostcodes)

  def postcodeFromSectorGen(sector: String) =
    Gen.listOfN(2, Gen.alphaNumChar).map(_.mkString).map(sector + _.toUpperCase)

  def randomTransactionGen(): Gen[Transaction] = for {
    timestamp <- timestampGen.map(_.getMillis)
    postcode <- postcodeGen
    businessId <- Gen.oneOf(businesses)
    customerId <- Gen.oneOf(customers)
  } yield Transaction(timestamp, customerId, businessId, Some(postcode))

  def randomTransactions(n: Int): Gen[List[Transaction]] = Gen.listOfN(nTransactions, randomTransactionGen())
}

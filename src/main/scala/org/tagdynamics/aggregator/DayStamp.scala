package org.tagdynamics.aggregator

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.TimeZone

import scala.util.Try

/**
 * Memory efficient storage of (year, month, day) UTC-timestamps using 16 bit
 * of memory per timestamp.
 *
 * Allowed ranges:
 *   year = 2000 .. 2063, month = 1 .. 12, day = 1 .. 31.
 *
 * The OSM project started ~2004 so this should not limit anything.
 *
 * Note: 16 bit should leave 7 bit for the year, and then hold up to ~2127,
 * but the below only works up to 2063 for some reason. Probably the first
 * MSB is treated as a sign bit somewhere (?)
 *
 * See:
 *  - https://wiki.openstreetmap.org/wiki/History_of_OpenStreetMap
 *  - https://docs.scala-lang.org/overviews/core/value-classes.html
 */
case class DayStamp(data: Char) extends AnyVal {
  def year: Int = (data >> (4 + 5 + 1)) + 2000
  def month: Int = (data >> 5) & ((1 << 4) - 1) // 4 bit of data
  def day: Int = data & ((1 << 5) - 1) // 5 bit of data

  // eg. yr = 2000, month = 1, day = 2 => "000102"
  // Note: sorting by `toString` puts elements in chronological order
  override def toString: String = f"${year - 2000}%02d$month%02d$day%02d"

  /** UTC epoch seconds (for 00:00:00) of given day */
  def epochSecs: Long = {
    val s = new SimpleDateFormat("yyyyMMdd")
    s.setTimeZone(TimeZone.getTimeZone("UTC"))
    s.parse(s"20$toString").getTime / 1000
  }
}

case object DayStamp {

  /** 1.1.2010 -> true, 31.2.2010 -> false */
  def isValidCalenderDate(year: Int, month: Int, day: Int): Boolean = {

    if (year < 2000 || year > 2063) throw new Exception(s"Year $year out of range")
    if (month < 1 || month > 12) throw new Exception(s"Month $month out of range")
    if (day < 1 || day > 31) throw new Exception(s"Day $day out of range")

    val s = new SimpleDateFormat("yyyyMMdd")
    s.setTimeZone(TimeZone.getTimeZone("UTC"))
    s.setLenient(false)

    Try { s.parse(year.toString + "%02d".format(month) + "%02d".format(day)) }.isSuccess
  }

  def from(year0: Int, month: Int, day: Int): DayStamp = {
    if (!isValidCalenderDate(year0, month, day)) throw new Exception(s"Date $year0/$month/$day not valid")

    val year = year0 - 2000
    val value = (year << (4 + 5 + 1)) + (month << (4 + 1)) + day
    DayStamp(value.toChar)
  }

  def from(yymmdd: String): DayStamp = {
    if (yymmdd.length != 6) throw new Exception(s"Expected YYMMDD, got $yymmdd")
    val yyyy = yymmdd.substring(0, 2).toInt + 2000
    val mm = yymmdd.substring(2, 4).toInt
    val dd = yymmdd.substring(4, 6).toInt
    DayStamp.from(yyyy, mm, dd)
  }

  def from(epochSeconds: Long): DayStamp = {
    val i = Instant.ofEpochSecond(epochSeconds)

    val foo = LocalDateTime.ofInstant(i, ZoneId.of("UTC"))

    val day: Int = foo.getDayOfMonth // 1 .. 31 (5 bit)
    val month: Int = foo.getMonthValue // 1 .. 12 (4 bit)
    val year: Int = foo.getYear
    DayStamp.from(year, month, day)
  }

}

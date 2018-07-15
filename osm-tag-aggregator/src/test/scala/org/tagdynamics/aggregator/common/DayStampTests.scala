package org.tagdynamics.aggregator.common

import org.junit.Assert._
import org.junit.Test

class DayStampTests {

  @Test
  def `Create from unix epoch (in secs)`(): Unit = {
    val ts = DayStamp.from(1103891096)
    assertEquals(2004, ts.year)
    assertEquals(12, ts.month)
    assertEquals(24, ts.day)

    assertEquals("041224", ts.toString)
  }

  @Test
  def `Create from YYMMDD string`(): Unit = {
    val ts = DayStamp.from("041224")
    assertEquals(2004, ts.year)
    assertEquals(12, ts.month)
    assertEquals(24, ts.day)
  }

  @Test
  def `Loop over lots of days`(): Unit = {
    for (year <- 2000 to 2063; month <- 1 to 12; day <- 1 to 31
         if DayStamp.isValidCalenderDate(year, month, day)) {
      val dayStamp = DayStamp.from(year, month, day)
      assertEquals(year, dayStamp.year)
      assertEquals(month, dayStamp.month)
      assertEquals(day, dayStamp.day)

      assertEquals(6, dayStamp.toString.length)
      assertEquals(dayStamp, DayStamp.from(dayStamp.toString))
      assertEquals(dayStamp.toString, DayStamp.from(dayStamp.toString).toString)
      assertEquals(dayStamp, DayStamp.from(dayStamp.epochSecs))
    }
  }

}

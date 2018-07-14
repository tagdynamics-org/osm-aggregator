package org.tagdynamics.aggregator.aggregators

import org.junit.Assert._
import org.junit.Test
import org.tagdynamics.aggregator.{DayStamp, JSONCustomProtocols}

// Tests for `TotalRevisionAggregator` and `LiveRevisionAggregator` both outputting `Counted[ElementState]`

class ElementStateCountingAggregatorTests {

  val day1 = DayStamp.from("140102")
  val day2 = DayStamp.from("140103")
  val day3 = DayStamp.from("140105")

  val del: ElementState = Deleted
  val es1: ElementState = Visible(List("0:v1"))
  val es2: ElementState = Visible(List("0:v1", "0:v2"))
  val es3: ElementState = Visible(List("0:v2", "0:v4", "1:v1"))
  // es4 never occurs in live data
  val es4: ElementState = Visible(List("0:v2", "0:v4", "1:v1", "9:v1123"))

  val input = List(
    EntryHistory("N1", List((day1, es1), (day1, es2))),
    EntryHistory("N2", List((day2, es1), (day2, del), (day3, es1))),
    EntryHistory("N3", List((day1, es1), (day1, es2), (day1, es1), (day2, del), (day3, es1))),
    EntryHistory("N4", List((day1, es3), (day2, es4), (day3, del))),
    EntryHistory("N5", List((day3, es3)))
  )

  // total number of map elements per state
  //  es1: {N1, N2, N3}, es2: {N1, N3}, es3: {N4, N5}, es4: {N4}, del: {N2, N3, N4}

  @Test
  def `LiveRevisionAggregator should compute correct result for small test data`(): Unit = {
    val p = LiveRevisionAggregator

    val expectedOutput: Set[Counted[ElementState]] = Set(
      Counted(es1, n = 2),
      Counted(es2, n = 1),
      Counted(es3, n = 1),
      Counted(del, n = 1)
    )

    assertEquals(expectedOutput, p.count(input.toIterator).toSet)
    assertEquals(Set[Counted[ElementState]](), p.apply(input.toIterator).toSet)
  }

  @Test
  def `TotalRevisionAggregator should compute correct result for small test data`(): Unit = {
    val p = TotalRevisionAggregator

    val expectedOutput: Set[Counted[ElementState]] = Set(
      Counted(es1, n = 3),
      Counted(es2, n = 2),
      Counted(es3, n = 2),
      Counted(es4, n = 1),
      Counted(del, n = 3)
    )

    assertEquals(expectedOutput, p.count(input.toIterator).toSet)
    assertEquals(Set[Counted[ElementState]](), p.apply(input.toIterator).toSet)
  }

  @Test
  def `can serialize/deserialize Counted[ElementState] objects`(): Unit = {

    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(obj: Counted[ElementState]): String = obj.toJson.toString
      def fromJson(line: String): Counted[ElementState] = line.parseJson.convertTo[Counted[ElementState]]
    }

    val obj = Counted[ElementState](key = NotCreated, n = 123)
    val json = """{"key":{"state":"NC","tags":[]},"n":123}"""

    assertEquals(obj, I.fromJson(json))
    assertEquals(json, I.toJson(obj))
  }

}

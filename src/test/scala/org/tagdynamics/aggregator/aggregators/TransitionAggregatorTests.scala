package org.tagdynamics.aggregator.aggregators

import org.junit.Assert._
import org.junit.Test
import org.tagdynamics.aggregator.JSONCustomProtocols
import org.tagdynamics.aggregator.common.DayStamp

class TransitionAggregatorTests {

  val day1 = DayStamp.from("160203")
  val day2 = DayStamp.from("160310")
  val day3 = DayStamp.from("200101")

  val nc: ElementState = NotCreated
  val del: ElementState = Deleted
  val es1: ElementState = Visible(List("0:v1"))
  val es2: ElementState = Visible(List("0:v1", "0:v2"))
  val es3: ElementState = Visible(List("0:v2", "0:v4", "1:v1"))

  val input = List(
    EntryHistory("N1", List((day1, es1), (day1, es2), (day1, es1), (day1, es2), (day2, del))),
    EntryHistory("N2", List((day2, es1), (day2, del), (day3, es1), (day3, es3))),
    EntryHistory("N3", List((day1, es2), (day1, es3), (day1, es1), (day2, es2), (day3, del))),
    EntryHistory("N4", List((day3, es3)))
  )

  val expectedOutput: Set[Counted[Transition[ElementState]]] = Set(
    // form es1
    Counted(key = Transition(from = es1, to = es2), n = 2),  //  N1(x2), N3
    Counted(key = Transition(from = es1, to = es3), n = 1),  //  N2
    Counted(key = Transition(from = es1, to = del), n = 1),  //  N2
    // from es2
    Counted(key = Transition(from = es2, to = es1), n = 1),  //  N1
    Counted(key = Transition(from = es2, to = es3), n = 1),  //  N3
    Counted(key = Transition(from = es2, to = del), n = 2),  //  N1, N3
    // from es3
    Counted(key = Transition(from = es3, to = es1), n = 1),  //  N3
    Counted(key = Transition(from = es3, to = es2), n = 0),  //
    Counted(key = Transition(from = es3, to = del), n = 0),  //
    // from del
    Counted(key = Transition(from = del, to = es1), n = 1),  //  N2
    Counted(key = Transition(from = del, to = es2), n = 0),  //
    Counted(key = Transition(from = del, to = es3), n = 0),  //
    // from nc
    Counted(key = Transition(from = nc, to = es1), n = 2),   // N1, N2
    Counted(key = Transition(from = nc, to = es2), n = 1),   // N3
    Counted(key = Transition(from = nc, to = es3), n = 1)    // N4
  ).filter(_.n > 0)

  @Test
  def `should pass for small test data`() {
    val p = TransitionsAggregator
    assertEquals(expectedOutput, p.count(input.toIterator).toSet)
  }

  @Test
  def `JSON: can serialize/deserialize Counted[Transition[ElementState]] objects`(): Unit = {
    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(deltas: Counted[Transition[ElementState]]): String = deltas.toJson.toString
      def fromJson(line: String): Counted[Transition[ElementState]] = line.parseJson.convertTo[Counted[Transition[ElementState]]]
    }

    val obj: Counted[Transition[ElementState]] = expectedOutput.toSeq.head
    val json = I.toJson(obj)

    assert(json.length > 10)

    assertEquals(obj, I.fromJson(json))
    assertEquals(json, I.toJson(I.fromJson(json)))
  }

}

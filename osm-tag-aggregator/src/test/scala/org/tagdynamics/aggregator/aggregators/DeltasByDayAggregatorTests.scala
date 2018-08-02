package org.tagdynamics.aggregator.aggregators

import org.junit.Assert._
import org.junit.Test
import org.tagdynamics.aggregator.common._

class DeltasByDayAggregatorTests {

  @Test
  def `sumSignedDeltas for small data; do not store delta=0`() {
    val p = DeltasByDayAggregator

    val day1 = DayStamp.from("120102")
    val day2 = DayStamp.from("120103")
    val day3 = DayStamp.from("120104")

    val nc: ElementState = NotCreated
    val del: ElementState = Deleted
    val es: ElementState = Visible(List("0:crossing", "a:road"))

    val input: Seq[Counted[DeltaCount]] = Seq(
      // day 1: es=+1; nc=+9; del=+2
      Counted(Increase(day1, es), 1),
      Counted(Increase(day1, nc), 10),
      Counted(Decrease(day1, nc), 1),
      Counted(Increase(day1, del), 1),
      Counted(Increase(day1, del), 1),
      // day 2: es=-7
      Counted(Decrease(day2, es), 5),
      Counted(Decrease(day2, es), 1),
      Counted(Increase(day2, es), 1),
      Counted(Decrease(day2, es), 1),
      Counted(Decrease(day2, es), 1),
      // day 3: es=+1, del=0
      Counted(Increase(day3, es), 2),
      Counted(Decrease(day3, es), 1),
      Counted(Increase(day3, del), 1),
      Counted(Decrease(day3, del), 1)
    )

    val expectedOutput: Seq[(ElementState, (DayStamp, Int))] = Seq(
      (es, (day1, +1)),
      (es, (day2, -7)),
      (es, (day3, +1)),
      (nc, (day1, +9)),
      (del, (day1, +2)),
      (del, (day3, 0)) // delta=0 update
    )

    assertEquals(expectedOutput.toSet, p.sumSignedDeltas(input).toSet)
  }

  @Test
  def `small test data`() {
    val p = DeltasByDayAggregator

    val day1 = DayStamp.from("160203")
    val day2 = DayStamp.from("160310")
    val day3 = DayStamp.from("160615")

    val del: ElementState = Deleted
    val es1: ElementState = Visible(List("0:crossing", "a:road"))
    val es2: ElementState = Visible(List("0:crossing", "a:footpath"))
    val es3: ElementState = Visible(List("0:crossing", "a:foot_path"))

    val input = List(
      EntryHistory("N1", List((day1, es1), (day1, es2))),
      EntryHistory("N2", List((day2, es1), (day2, del), (day3, es1))),
      EntryHistory("N3", List((day1, es1), (day1, es2), (day1, es1), (day2, del), (day3, es1))),
      // es3 only appears on day3 as +1 -1; test that delta=0 is not recorded
      EntryHistory("N4", List((day3, es3), (day3, del)))
    )

    /**
     *       | day1         | day2         | day3         |
     *  -----+--------------+--------------+--------------+-
     *   es1 | +1-1+1-1+1=1 | +1-1-1=-1    | +1+1=+2      |
     *   es2 | +1+1-1=1     |              |              |
     *   es3 |              |              | +1-1=0       |
     *   del |              | +1+1=2       | -1-1+1=-1    |
     *  -----+--------------+--------------+--------------+-
     */

    val expectedOutput: Set[DeltasByDay[ElementState]] = Set(
      DeltasByDay(es1, Map(day1 -> 1, day2 -> -1, day3 -> 2)),
      DeltasByDay(es2, Map(day1 -> 1)),
      DeltasByDay(es3, Map(day3 -> 0)), // delta=0 updates
      DeltasByDay(del, Map(day2 -> 2, day3 -> -1))
    )

    assertEquals(expectedOutput, p.apply(input.toIterator).toSet)
  }

}

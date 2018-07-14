package org.tagdynamics.aggregator.aggregators

import org.junit.Assert._
import org.junit.Test
import org.tagdynamics.aggregator.{DayStamp, JSONCustomProtocols, Utils}
import spray.json._

import scala.io.Source

// The below tests assert that the output of the main processors produce the same output as
// in some cached precomputed output files from synthetic input data.

class IOLockTests extends JSONCustomProtocols {

  def aggPath(file: String): String = s"/testdata/3-aggregates/$file.jsonl"

  /** Return iterator with extracted synthetic OSM revision histories*/
  def dataIterator(): Iterator[EntryHistory] = {
    val source = Source.fromURI(getClass.getResource("/testdata/2-extracted-osm-metadata.jsonl").toURI)

    val input: List[EntryHistory] = source.getLines().map(EntryHistory.deserialize).toList
    source.close()
    input.toIterator
  }

  def parseCES(line: String): Counted[ElementState] = line.parseJson.convertTo[Counted[ElementState]]

  val liveRevisionCounts: Seq[Counted[ElementState]] = LiveRevisionAggregator.apply(dataIterator())

  @Test
  def `LiveRevisionAggregator: IO lock`() {
    val expectedOutput: Seq[Counted[ElementState]] = Utils.loadResourceJSONL(aggPath("live-revcounts"), parseCES)
    assertEquals(expectedOutput.toSet, liveRevisionCounts.toSet)
  }

  val totalRevisionCounts: Seq[Counted[ElementState]] = TotalRevisionAggregator.apply(dataIterator())

  @Test
  def `TotalRevisionAggregator: IO lock`() {
    val expectedOutput: Seq[Counted[ElementState]] = Utils.loadResourceJSONL(aggPath("total-revcounts"), parseCES)
    assertEquals(expectedOutput.toSet, totalRevisionCounts.toSet)
  }

  def asMap(xs: Seq[Counted[ElementState]]): Map[ElementState, Int] = xs.map(x => (x.key, x.n)).toMap

  @Test
  def `0 < LiveRevisionCount(element) <= TotalRevisionCount(element) for live elements`() {
    val liveMap = asMap(liveRevisionCounts)
    val totalMap = asMap(liveRevisionCounts)

    assert(liveMap.keySet.subsetOf(totalMap.keySet))

    for (state <- liveMap.keySet) {
      assert(0 < liveMap(state))
      assert(liveMap(state) <= totalMap(state))
    }
  }

  @Test
  def `TransitionsAggregator: IO lock`() {
    def parse(line: String): Counted[Transition[ElementState]] = line.parseJson.convertTo[Counted[Transition[ElementState]]]

    val output: Seq[Counted[Transition[ElementState]]] = TransitionsAggregator.apply(dataIterator())

    val expectedOutput: Seq[Counted[Transition[ElementState]]] = Utils.loadResourceJSONL(aggPath("transition-counts"), parse)
    assertEquals(expectedOutput.toSet, output.toSet)
  }

  val deltaPerDayOutput: Seq[DeltasByDay[ElementState]] = DeltasByDayAggregator.apply(dataIterator())

  @Test
  def `DeltaByDayAggregator: IO lock`() {
    def parse(line: String): DeltasByDay[ElementState] = {
      import spray.json._
      line.parseJson.convertTo[DeltasByDay[ElementState]]
    }

    val expectedOutput: Seq[DeltasByDay[ElementState]] = Utils.loadResourceJSONL(aggPath("per-day-delta-counts"), parse)
    assertEquals(expectedOutput.toSet, deltaPerDayOutput.toSet)
  }

  @Test
  def `DeltaByDayAggregator: total count should never be negative for any element`() {
    // for a sequence x0, x1, .. assert that x_0 + ... + x_i is positive for all i
    def ensurePositiveSums(xs: Seq[Int]): Unit = {
      for (i <- 1 to xs.length) {
        assert(xs.take(i).sum >= 0)
      }
    }

    for (x: DeltasByDay[ElementState] <- deltaPerDayOutput) {
      val sortedDeltas: Seq[(DayStamp, Int)] = x.deltas.toSeq.sortBy{ case (day, _) => day.toString }
      ensurePositiveSums(sortedDeltas.map(_._2))
    }
  }

  @Test
  def `DeltaByDayAggregator: Sum(Deltas(element)) = LiveCount(element)`() {

    // We need the full aggregated data, not the filtered data where counts <5 are removed.
    val liveMap = asMap(LiveRevisionAggregator.count(dataIterator()))
    val totalMap = asMap(TotalRevisionAggregator.count(dataIterator()))

    for (x: DeltasByDay[ElementState] <- deltaPerDayOutput) {
      val summedDeltas: Int = x.deltas.toSeq.map(_._2).sum

      if (liveMap.contains(x.key)) {
        assertEquals(liveMap(x.key), summedDeltas)
        assert(liveMap(x.key) > 0)
      } else {
        assert(totalMap.keySet.contains(x.key))
        assertEquals(0, summedDeltas)
      }
    }
  }

  // TODO: integrating the output of the DeltaPerDay processor should give the same result as
  // cutting of the input data at some date and computing live counts using LatestRevisionProcessor

}

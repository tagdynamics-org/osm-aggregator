package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.{Counted, DayStamp, ElementState}
import org.tagdynamics.aggregator.{JSONInputReader, StreamKeyCounter}

/**
 * Input data: each input JSONL line is deserialized into a `EntryHistory` object.
 */
case class EntryHistory(tid: String, history: List[(DayStamp, ElementState)])

case object EntryHistory extends JSONInputReader {
  import spray.json._

  def deserialize(line: String): EntryHistory = line.parseJson.convertTo[EntryHistory]
}

trait Aggregator {

  // Type of what we are counting
  type Key

  // Output JSONL line type, and how to serialize this
  type OutputLine

  def toJson(out: OutputLine): String

  // Extract list of `Key`:s to count from one map elements entire version history
  def extractKeys(x: EntryHistory): Seq[Key]

  final def count(xs: Iterator[EntryHistory]): Seq[Counted[Key]] = {
    StreamKeyCounter
      .keyCounter(xs, (revs: EntryHistory) => extractKeys(revs))
      .view.map(kv => Counted(kv._1, kv._2))
      .toSeq
  }

  def postProsessor(xs: Seq[Counted[Key]]): Seq[OutputLine]

  /** compute aggregation pipeline */
  final def apply(xs: Iterator[EntryHistory]): Seq[OutputLine] = postProsessor(count(xs))

}

package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.DayStamp
import org.tagdynamics.aggregator.{JSONInputReader, StreamKeyCounter}

/**
 * Describe the different states a map element (or map element revision) can be in
 *
 * Notes:
 *  - tagList can be empty in Visible state
 *  - NotCreated is only used to output transitions
 */
sealed trait ElementState
case object NotCreated extends ElementState
case class Visible(tagList: List[String]) extends ElementState
case object Deleted extends ElementState

/**
 * Input data: each input JSONL line is deserialized into a `EntryHistory` object.
 */
case class EntryHistory(tid: String, history: List[(DayStamp, ElementState)])

case object EntryHistory extends JSONInputReader {
  import spray.json._

  def deserialize(line: String): EntryHistory = line.parseJson.convertTo[EntryHistory]
}

/** Used for many output JSONL files, and for intermediate steps */
case class Counted[A](key: A, n: Int)

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
      .map(kv => Counted(kv._1, kv._2))
      .toSeq
  }

  def postProsessor(xs: Seq[Counted[Key]]): Seq[OutputLine]

  /** compute aggregation pipeline */
  final def apply(xs: Iterator[EntryHistory]): Seq[OutputLine] = postProsessor(count(xs))

}

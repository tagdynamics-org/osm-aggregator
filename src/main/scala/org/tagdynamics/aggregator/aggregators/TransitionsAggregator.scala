package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.JSONCustomProtocols

case class Transition[A](from: A, to: A)

/**
 *  TransitionsAggregator
 *
 *  For each element state transition, say, (Visible, Tags=(a)) -> Deleted, count
 *  how many unique map elements have done that transition.
 *
 *  Creating a map element is described as a `NotCreated -> elementState`
 *  transition.
 *
 */
object TransitionsAggregator extends Aggregator with JSONCustomProtocols {
  import spray.json._

  override type Key = Transition[ElementState]
  override type OutputLine = Counted[Transition[ElementState]]

  override def extractKeys(revs: EntryHistory): Seq[Key] = {
    if (revs.history.isEmpty) {
      throw new Exception(s"Got history entry with length zero : $revs")
    } else {
      (NotCreated +: revs.history.map(_._2)).sliding(2).map {
        case List(from, to) => Transition(from = from, to = to)
      }.toSeq.distinct
    }
  }

  override def postProsessor(xs: Seq[Counted[Key]]): Seq[Counted[Key]] = xs.filter(x => x.n >= 5).sortBy(x => -x.n)

  override def toJson(out: OutputLine): String = out.toJson.toString

}

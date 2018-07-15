package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.{Counted, ElementState, JSONCustomProtocols}

/**
 * LiveRevisionAggregator
 *
 * For each elementState record total number of map elements that are live (present in latest
 * version).
 *
 * Note.
 *  - `Deleted` is a valid element state. This allows us to track total number of map elements
 *    that are currently deleted.
 *  - Similarly, the state visible and `tags = []` is also a valid element state. This is only
 *    recorded for map elements that at some point in their history have had any of the selected
 *    tags.
 */
object LiveRevisionAggregator extends Aggregator with JSONCustomProtocols {
  import spray.json._

  override type Key = ElementState
  override type OutputLine = Counted[ElementState]

  override def extractKeys(x: EntryHistory): Seq[Key] = Seq(x.history.last._2)

  override def postProsessor(xs: Seq[Counted[Key]]): Seq[Counted[Key]] = xs.filter(x => x.n >= 5).sortBy(x => -x.n)

  override def toJson(out: Counted[ElementState]): String = out.toJson.toString

}

package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.{Counted, ElementState, JSONCustomProtocols}

/**
 *  TotalRevisionAggregator
 *
 *  Count total number of map element counts for each tag state. IE. how many **map elements**
 *  have had a tag over the entire history of the OSM.
 *
 *  Note: if a map element N1 switches between two different states, say tagA and tagB, 10 times,
 *  N1 increases the count for tags A and B by 1, not by 10.
 */
object TotalRevisionAggregator extends Aggregator with JSONCustomProtocols {
  import spray.json._

  override type Key = ElementState
  override type OutputLine = Counted[ElementState]

  override def extractKeys(x: EntryHistory): Seq[Key] = x.history.map(_._2).distinct

  override def postProsessor(xs: Seq[Counted[Key]]): Seq[Counted[Key]] = xs.filter(x => x.n >= 5).sortBy(x => -x.n)

  def toJson(out: Counted[ElementState]): String = out.toJson.toString
}

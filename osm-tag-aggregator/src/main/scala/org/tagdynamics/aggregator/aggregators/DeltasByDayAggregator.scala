package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.{Counted, DayStamp, DeltasByDay, ElementState, JSONCustomProtocols}

sealed trait DeltaCount
final case class Increase(ts: DayStamp, elementState: ElementState) extends DeltaCount
final case class Decrease(ts: DayStamp, elementState: ElementState) extends DeltaCount

object DeltasByDayAggregator extends Aggregator with JSONCustomProtocols {
  import spray.json._

  override type Key = DeltaCount
  override type OutputLine = DeltasByDay[ElementState]

  def sumSignedDeltas(xs: Seq[Counted[DeltaCount]]): Seq[(ElementState, (DayStamp, Int))] = {
    def getK(x: DeltaCount): (ElementState, DayStamp) = x match {
      case Increase(ts, es) => (es, ts)
      case Decrease(ts, es) => (es, ts)
    }

    def signedCount(x: Counted[DeltaCount]): Int = x.key match {
      case Increase(_, _) => 1 * x.n
      case Decrease(_, _) => -1 * x.n
    }

    val foo: Map[(ElementState, DayStamp), Seq[Counted[DeltaCount]]] = xs.groupBy((x: Counted[DeltaCount]) => getK(x.key))

    foo.toSeq.view.map {
      case ((es, day), dcounts) => (es, (day, dcounts.view.map(signedCount).sum))
    }.filter { case (_, (_, n)) => n != 0 }
  }

  // Note: no filtering unlike in many other aggregators
  override def postProsessor(xs: Seq[Counted[DeltaCount]]): Seq[DeltasByDay[ElementState]] = {
    println(s"DeltasByDayAggregator: postProcessor, input size = ${xs.length}")
    var summed = sumSignedDeltas(xs)
    
    println(s"DeltasByDayAggregator: summedDeltas= ${summed.length}")
    val grouped: Map[ElementState, Seq[(ElementState, (DayStamp, Int))]] = summed.groupBy(x => x._1)
    summed = null // clear memory
    
    println(s"DeltasByDayAggregator: grouped= ${grouped.keySet.size}")
    (for { (es, deltasByEs) <- grouped }
      yield DeltasByDay(es, deltasByEs.view.map(x => x._2).toMap)).toSeq
  }

  // Extract Increase/Decrease events from the revision history
  override def extractKeys(x: EntryHistory): Seq[Key] = {
    if (x.history.isEmpty) {
      throw new Exception(s"Got history entry with length zero : $x")
    } else if (x.history.length == 1) {
      val (day, elementState) = x.history.head
      Seq(Increase(day, elementState))
    } else {
      x.history.sliding(2).flatMap {
        case List(from, to) =>
          Seq(
            Increase(from._1, from._2),
            Decrease(to._1, from._2)
          )
      }.toSeq ++ {
        val (day, elementState) = x.history.last
        Seq(Increase(day, elementState))
      }
    }
  }

  override def toJson(out: OutputLine): String = out.toJson.toString
}

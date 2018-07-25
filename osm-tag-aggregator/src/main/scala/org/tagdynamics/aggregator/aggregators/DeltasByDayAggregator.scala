package org.tagdynamics.aggregator.aggregators

import org.tagdynamics.aggregator.common.{Counted, DayStamp, DeltasByDay, ElementState, JSONCustomProtocols}

sealed trait DeltaCount
final case class Increase(ts: DayStamp, elementState: ElementState) extends DeltaCount
final case class Decrease(ts: DayStamp, elementState: ElementState) extends DeltaCount

object DeltasByDayAggregator extends Aggregator with JSONCustomProtocols {
  import spray.json._

  override type Key = DeltaCount
  override type OutputLine = DeltasByDay[ElementState]

  def sumSignedDeltas(xs: Seq[Counted[DeltaCount]]): Seq[(ElementState, DayStamp, Int)] = {
    case class Timed[A](key: A, ts: DayStamp)

    def getK(x: DeltaCount): Timed[ElementState] = x match {
      case Increase(ts, es) => Timed(es, ts)
      case Decrease(ts, es) => Timed(es, ts)
    }

    def signedCount(x: Counted[DeltaCount]): Int = x.key match {
      case Increase(_, _) => 1 * x.n
      case Decrease(_, _) => -1 * x.n
    }

    (for { (k, ys) <- xs.groupBy((x: Counted[DeltaCount]) => getK(x.key)) }
      yield (k.key, k.ts, ys.view.map(signedCount).sum))
      .filter(_._3 != 0) // do not output delta=0
      .toSeq
  }

  // Note: no filtering unlike in many other aggregators
  override def postProsessor(xs: Seq[Counted[DeltaCount]]): Seq[DeltasByDay[ElementState]] = {
    (for { (es, deltasByEs) <- sumSignedDeltas(xs).groupBy(x => x._1) }
      yield DeltasByDay(es, deltasByEs.view.map(x => (x._2, x._3)).toMap)).toSeq
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

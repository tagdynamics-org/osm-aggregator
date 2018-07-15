package org.tagdynamics.aggregator

import org.tagdynamics.aggregator.aggregators._
import org.tagdynamics.aggregator.common.{DayStamp, Deleted, ElementState, Visible}
import spray.json.DefaultJsonProtocol

/** JSON support for reading input JSONL files (each line is deserialized into a `EntryHistory`) */
trait JSONInputReader extends DefaultJsonProtocol {
  import spray.json._

  type Version = Int // never used. Remove form input JSONL format?
  type Epoch = Long // seconds since 1970
  type OutTriplet = (Epoch, Version, ElementState)

  implicit object TagStateJsonFormat extends RootJsonReader[OutTriplet] {
    def read(value: JsValue): OutTriplet = value match {
      case JsArray(Vector(JsNumber(ts), JsNumber(version), JsNumber(visible), JsArray(tags))) => {
        visible.toInt match {
          case 0 => (ts.toLong, version.toInt, Deleted)
          case 1 => (ts.toLong, version.toInt, Visible(tags.map(_.convertTo[String]).toList))
          case _ => throw new Exception("Visible should be 0 (=false) or 1 (=true)")
        }
      }
      case _ => throw new Exception("TagState expected")
    }
  }

  implicit object EntryHistoryJsonFormat extends RootJsonReader[EntryHistory] {
    def read(value: JsValue): EntryHistory = value match {
      case JsArray(Vector(JsString(tid), JsArray(hist))) => {
        val x: List[OutTriplet] = hist.map(e => TagStateJsonFormat.read(e)).toList

        if (x.isEmpty) throw new Exception("Found entry with empty history")

        EntryHistory(tid.toString, x.map(r => (DayStamp.from(r._1), r._3)))
      }
      case _ => throw new Exception("EntryHistory expected")
    }
  }
}

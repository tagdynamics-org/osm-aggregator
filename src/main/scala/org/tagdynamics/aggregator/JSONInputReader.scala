package org.tagdynamics.aggregator

import org.tagdynamics.aggregator.aggregators._
import org.tagdynamics.aggregator.common.DayStamp
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}

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

/** JSON support for everything else */
trait JSONCustomProtocols extends DefaultJsonProtocol {

  /** Need to serialize `ElementState` when writing output from various processors */
  implicit object TagStateJsonFormat extends RootJsonFormat[ElementState] {
    def write(x: ElementState): JsObject = x match {
      case NotCreated => JsObject("state" -> JsString("NC"), "tags" -> JsArray(Vector[JsString]()))
      case Deleted => JsObject("state" -> JsString("DEL"), "tags" -> JsArray(Vector[JsString]()))
      case Visible(tagList) => JsObject("state" -> JsString("VIS"), "tags" -> JsArray(tagList.toVector.map(JsString(_))))
    }

    def read(value: JsValue): ElementState = value.asJsObject.getFields("state", "tags") match {
      case Seq(JsString(state), JsArray(tags)) => state match {
        case "DEL" => Deleted
        case "NC" => NotCreated
        case "VIS" => Visible(tags.map(_.convertTo[String]).toList)
      }
      case _ => throw new Exception("Parse error")
    }
  }

  implicit object DayStampFormat extends RootJsonFormat[DayStamp] {
    def write(x: DayStamp): JsValue = JsString(x.toString)
    def read(value: JsValue): DayStamp = DayStamp.from(value.convertTo[String])
  }

  // output serializers for the various processors
  implicit val j1 = jsonFormat2(Counted[ElementState])
  implicit val j2 = jsonFormat2(Transition[ElementState])
  implicit val j3 = jsonFormat2(Counted[Transition[ElementState]])
  implicit val j4 = jsonFormat2(DeltasByDay[ElementState])

}

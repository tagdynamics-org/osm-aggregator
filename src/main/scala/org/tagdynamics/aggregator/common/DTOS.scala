package org.tagdynamics.aggregator.common

import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}

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

case class Counted[A](key: A, n: Int)
case class Transition[A](from: A, to: A)
case class DeltasByDay[A](key: A, deltas: Map[DayStamp, Int])

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

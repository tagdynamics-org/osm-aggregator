package org.tagdynamics.aggregator.common

import org.junit.Assert._
import org.junit.Test

class JSONTests {

  @Test
  def `can serialize/deserialize {NotCreated, Deleted, Visible(tagList)} ElementState:s`(): Unit = {
    // Note: variables `nc`, `del`, `es` must be typed as `ElementState`
    val nc: ElementState = NotCreated
    val ncJson: String = """{"state":"NC","tags":[]}"""

    val del: ElementState = Deleted
    val delJson: String = """{"state":"DEL","tags":[]}"""

    val es: ElementState = Visible(List("0:crossing", "a:road"))
    val esJson: String = """{"state":"VIS","tags":["0:crossing","a:road"]}"""

    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(es: ElementState): String = es.toJson.toString
      def fromJson(line: String): ElementState = line.parseJson.convertTo[ElementState]
    }

    assertEquals(ncJson, I.toJson(nc))
    assertEquals(delJson, I.toJson(del))
    assertEquals(esJson, I.toJson(es))

    assertEquals(nc, I.fromJson(ncJson))
    assertEquals(del, I.fromJson(delJson))
    assertEquals(es, I.fromJson(esJson))
  }

  @Test
  def `can serialize/deserialize DayStamp`(): Unit = {
    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(ds: DayStamp): String = ds.toJson.toString
      def fromJson(line: String): DayStamp = line.parseJson.convertTo[DayStamp]
    }

    val x = DayStamp.from("141130")
    val xJson: String = I.toJson(x)
    assertEquals(""""141130"""", xJson)
    assertEquals(x, I.fromJson(xJson))
  }

  @Test
  def `can serialize/deserialize Counted[ElementState]`(): Unit = {
    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(obj: Counted[ElementState]): String = obj.toJson.toString
      def fromJson(line: String): Counted[ElementState] = line.parseJson.convertTo[Counted[ElementState]]
    }

    val obj = Counted[ElementState](key = NotCreated, n = 123)
    val json = """{"key":{"state":"NC","tags":[]},"n":123}"""

    assertEquals(obj, I.fromJson(json))
    assertEquals(json, I.toJson(obj))
  }

  @Test
  def `JSON: can serialize/deserialize Counted[Transition[ElementState]] objects`(): Unit = {
    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(deltas: Counted[Transition[ElementState]]): String = deltas.toJson.toString
      def fromJson(line: String): Counted[Transition[ElementState]] = line.parseJson.convertTo[Counted[Transition[ElementState]]]
    }

    val json =
      """
        |{"key":{"from":{"state":"NC","tags":[]},"to":{"state":"VIS","tags":["1:v1","2:v3"]}},"n":40}
      """.stripMargin
    val obj: Counted[Transition[ElementState]] = I.fromJson(json)

    assertEquals(json, I.toJson(obj))
    assertEquals(obj, I.fromJson(I.toJson(obj)))
  }

  @Test
  def `JSON: can serialize/deserialize DeltasByDay[ElementState]`(): Unit = {
    object I extends JSONCustomProtocols {
      import spray.json._
      def toJson(deltas: DeltasByDay[ElementState]): String = deltas.toJson.toString
      def fromJson(line: String): DeltasByDay[ElementState] = line.parseJson.convertTo[DeltasByDay[ElementState]]
    }

    val day1 = DayStamp.from("041224")
    val day2 = DayStamp.from("041225")
    val day3 = DayStamp.from("041226")

    // Note: the type `: DeltasByDay[ElementState]` is needed below. No serializer is eg.
    // found for DeltasByDay[Visible]
    val deltas: DeltasByDay[ElementState] = DeltasByDay(
      Visible(List("a:1")),
      Map(
        day1 -> 1,
        day2 -> -1,
        day3 -> 2
      )
    )

    val expectedJson = """{"key":{"state":"VIS","tags":["a:1"]},"deltas":{"041224":1,"041225":-1,"041226":2}}"""
    assertEquals(expectedJson, I.toJson(deltas))
    assertEquals(deltas, I.fromJson(expectedJson))
  }

}
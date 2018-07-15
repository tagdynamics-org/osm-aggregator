package org.tagdynamics.aggregator

import org.junit.Assert._
import org.junit.Test
import org.tagdynamics.aggregator.aggregators._
import org.tagdynamics.aggregator.common.{DayStamp, Deleted, ElementState, JSONCustomProtocols, NotCreated, Visible}
import spray.json._

class JSONTests extends JSONCustomProtocols {

  @Test
  def `Spray JSON library can handle tuples`(): Unit ={
    val obj: (Int, List[Int]) = (1, List(1,2))
    val json: String = "[1,[1,2]]"
    assertEquals(json, obj.toJson.toString)
    assertEquals(obj, json.parseJson.convertTo[(Int, List[Int])])
  }

  // Note: variables `nc`, `del`, `es` must be typed as `ElementState`
  val nc: ElementState = NotCreated
  val ncJson: String = """{"state":"NC","tags":[]}"""

  val del: ElementState = Deleted
  val delJson: String = """{"state":"DEL","tags":[]}"""

  val es: ElementState = Visible(List("0:crossing", "a:road"))
  val esJson: String = """{"state":"VIS","tags":["0:crossing","a:road"]}"""

  @Test
  def `can serialize {NotCreated, Deleted, Visible(tagList)} ElementState types`(): Unit = {
    assertEquals(ncJson, nc.toJson.toString)
    assertEquals(delJson, del.toJson.toString)
    assertEquals(esJson, es.toJson.toString)
  }

  @Test
  def `can deserialize {NotCreated, Deleted, Visible(tagList)} ElementState types`(): Unit = {
    assertEquals(nc, ncJson.parseJson.convertTo[ElementState])
    assertEquals(del, delJson.parseJson.convertTo[ElementState])
    assertEquals(es, esJson.parseJson.convertTo[ElementState])
  }

  val d1 = DayStamp.from("160101")
  val d2 = DayStamp.from("160102")
  val d3 = DayStamp.from("160103")

  @Test
  def `deserialize EntryHistory (input file format): all visible`(): Unit = {
    val histJson =
      s"""
         |["T1283",[[${d1.epochSecs},1,1,["a","b"]],[${d2.epochSecs},2,1,["c"]],[${d3.epochSecs},3,1,[]]]]
         |""".stripMargin
    val deser: EntryHistory = EntryHistory.deserialize(histJson)

    val expected = EntryHistory(
      tid = "T1283",
      history = List(
        (d1, Visible(List("a", "b"))),
        (d2, Visible(List("c"))),
        (d3, Visible(List()))
      )
    )
    assertEquals(expected, deser)
  }

  @Test
  def `deserialize EntryHistory (input file format): some deleted`(): Unit = {
    val exampleInputLine =
      """
        |["N20",[[1317537081,1,1,["2:v2"]],[1319761077,2,0,[]],[1321355174,3,1,["1:v2","2:v5"]]]]
      """.stripMargin

    val exLine = EntryHistory.deserialize(exampleInputLine)
    val expected = EntryHistory("N20",
      List(
        (DayStamp.from("111002"), Visible(List("2:v2"))),
        (DayStamp.from("111028"), Deleted),
        (DayStamp.from("111115"), Visible(List("1:v2", "2:v5")))
      )
    )
    assertEquals(expected, exLine)
  }

}

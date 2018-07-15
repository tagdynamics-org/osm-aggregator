package org.tagdynamics.aggregator

import java.nio.file.{Files, Paths}

import scala.io.Source

object Utils {

  /** Write data to a file in UTF8 encoding */
  def writeFile[A](filename: String, xs: Seq[A], serialize: A => String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(filename))
    xs.foreach(e => writer.write(serialize(e) + "\n"))
    writer.flush()
    writer.close()
  }

  /** Non-streaming JSONL loader */
  def loadJSONL[A](filename: String, parse: String => A): Seq[A] = {
    val source = Source.fromFile(filename, "utf8")
    val result: Seq[A] = source.getLines().map(parse).toList
    source.close()
    result
  }

  /** Non-streaming JSONL loader */
  def loadResourceJSONL[A](resourceFilename: String, parse: String => A): Seq[A] = {
    // TODO: https://alvinalexander.com/scala/how-to-open-read-text-files-in-scala-cookbook-examples
    val source = Source.fromURI(getClass.getResource(resourceFilename).toURI)
    val result = source.getLines().map(parse).toList
    source.close()
    result
  }

}

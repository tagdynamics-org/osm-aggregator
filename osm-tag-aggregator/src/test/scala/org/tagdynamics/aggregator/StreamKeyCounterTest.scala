package org.tagdynamics.aggregator

import org.junit.Assert._
import org.junit.Test

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.Random

class StreamKeyCounterTest {
  // Non-streaming implementation of key counter.
  def countValuesGroupBy[A](xs: Seq[A]): HashMap[A, Int] = {
    val map: Map[A, Int] = xs.groupBy(x => x).mapValues(vec => vec.length)
    HashMap(map.toList : _*)
  }

  val r = new Random()
  def randomString(stringLength: Int): String = r.alphanumeric.take(stringLength).toList.mkString("")

  def testData(n: Int, stringLength: Int): Seq[Seq[String]] =
    (1 to n).map(_ => (1 to 1 + r.nextInt(10)).map(_ => randomString(stringLength)))

  @Test
  def `StreamKeyCounter.keyCounter`() {
    val input = testData(10000, 3)

    val flatKeys: Seq[String] = for { x <- input; y <- x } yield y

    val expected: HashMap[String, Int] = countValuesGroupBy(flatKeys)
    val testResult: HashMap[String, Int] = StreamKeyCounter.keyCounter(
      input.toIterator,
      (e: Seq[String]) => e,
      batchSize = 2000)

    assert(expected.values.max > 1)
    assertEquals(expected, testResult)
  }

  @Test
  def `StreamKeyCounter.keyCounter: extractKeys = lastKey`() {
    val input = testData(10000, 3)

    val expected: HashMap[String, Int] = countValuesGroupBy(input.map(_.last))
    val testResult: HashMap[String, Int] = StreamKeyCounter.keyCounter(
      input.toIterator,
      (e: Seq[String]) => Seq(e.last),
      batchSize = 2000)

    assert(expected.values.max > 1)
    assertEquals(expected, testResult)
  }

  @Test
  def `StreamKeyCounter.keyCounter: extractKeys = empty list`() {
    val input = testData(10000, 3)

    val testResult: HashMap[String, Int] = StreamKeyCounter.keyCounter(
      input.toIterator, (_: Seq[String]) => Seq(), batchSize = 2000)

    assertEquals(0, testResult.keySet.size)
  }

  @Test
  def `StreamKeyCounter.keyCounter: batchSize > input size`() {
    val input = testData(10000, 3)

    val flatKeys: Seq[String] = for { x <- input; y <- x } yield y

    val expected: HashMap[String, Int] = countValuesGroupBy(flatKeys)
    val testResult: HashMap[String, Int] = StreamKeyCounter.keyCounter(
      input.toIterator,
      (e: Seq[String]) => e,
      batchSize = Int.MaxValue)

    assert(expected.values.max > 1)
    assertEquals(expected, testResult)
  }

  @Test
  def `StreamKeyCounter.keyCounter: batchSize = 1`() {
    val input = testData(100, 1)

    val flatKeys: Seq[String] = for { x <- input; y <- x } yield y

    val expected: HashMap[String, Int] = countValuesGroupBy(flatKeys)
    val testResult: HashMap[String, Int] = StreamKeyCounter.keyCounter(
      input.toIterator,
      (e: Seq[String]) => e,
      batchSize = 1)

    assert(expected.values.max > 1)
    assertEquals(expected, testResult)
  }

  @Test
  def `keys with lots of repeats`() {

    var testEntries: Seq[Int] = {
      val res = ListBuffer[Int]()
      def add(value: Int, repeat: Int): Unit = (1 to repeat).foreach(_ => res += value)

      add(1, 1000)
      add(2, 5000)
      add(4, 25000)
      add(5, 100000)
      add(6, 250000)
      Random.shuffle(res)
    }

    val table1 = StreamKeyCounter.countValues(testEntries.par)
    val table2 = countValuesGroupBy(testEntries)

    assertEquals(table2, table1)
    assertEquals(Map[Int, Int](1 -> 1000, 2 -> 5000, 4 -> 25000, 5 -> 100000, 6 -> 250000), table1)
  }
}

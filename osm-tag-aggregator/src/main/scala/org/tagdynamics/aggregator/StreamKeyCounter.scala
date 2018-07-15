package org.tagdynamics.aggregator

import scala.collection.immutable.HashMap
import scala.collection.parallel.ParSeq

object StreamKeyCounter {

  /** Merge maps; adding values with common key */
  def mergeMaps[A](m1: HashMap[A, Int], m2: HashMap[A, Int]): HashMap[A, Int] = {
    m1.merged(m2) {
      // how to combine values for common keys (k1 = k2)
      case ((k1, v1), (_, v2)) => (k1, v1 + v2)
    }
  }

  /** How to add one element to a map */
  def seqOp[A](countMap: HashMap[A, Int], elem: A): HashMap[A, Int] = {
    countMap + (elem -> (countMap.getOrElse(elem, 0) + 1))
  }

  /**
   * From a sequence of values [x1, x2, ...] create
   *   HashMap(
   *     x1 -> <total counts for x1>,
   *     x2 -> <total counts for x2>,
   *     ...
   *   )
   */
  def countValues[A](xs: ParSeq[A]): HashMap[A, Int] = xs.aggregate(HashMap.empty[A, Int])(seqOp, mergeMaps)

  /**
   * A streaming key counter
   *
   * Note: performance is sensitive to the parallelization batch size. Too small or too large
   * will decrease performance
   */
  def keyCounter[A, B](xs: Iterator[A],
                       extractKeys: A => Seq[B],
                       batchSize: Int = 100000): HashMap[B, Int] = {

    /*
    // TODO: would a direct call to aggregate be faster?
    xs.aggregate(HashMap.empty[ElementState, Int])(Utils.seqOp[ElementState], Utils.mergeMaps)
    )*/

    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0")
    }

    // - Batch incoming entries.
    // - Extract keys in parallel
    // - Compute occurrences in each batch in parallel
    val batchedInput: Iterator[HashMap[B, Int]] =
    xs.grouped(batchSize)
      .map((arr: Seq[A]) => {
        val keys: ParSeq[B] = arr.par.flatMap(extractKeys)

        /*
        val m = keys.groupBy(x => x).mapValues(x => x.length).seq
        HashMap[B, Int](m.toSeq : _*)
        */

        StreamKeyCounter.countValues(keys)
      })

    var currentBatchNr = 0
    val x0 = HashMap.empty[B, Int]

    // Merge output from batched data by merging { key -> counts }-maps
    batchedInput.foldLeft(x0)((collectedMap, batchMap) => {
      currentBatchNr += 1
      println(s"   > batch $currentBatchNr: x = ${collectedMap.keySet.size}, y = ${batchMap.keySet.size}")
      val res = StreamKeyCounter.mergeMaps(collectedMap, batchMap) // not parallel
      res
    })
  }

}

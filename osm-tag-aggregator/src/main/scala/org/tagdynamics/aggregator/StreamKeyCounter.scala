package org.tagdynamics.aggregator

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import org.tagdynamics.sal.WorkBalancer

import scala.collection.immutable.HashMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamKeyCounter {

  /** Merge maps; adding values with common key */
  def mergeMaps[A](m1: HashMap[A, Int], m2: HashMap[A, Int]): HashMap[A, Int] = {
    m1.merged(m2) {
      // how to combine values for common keys (k1 = k2)
      case ((k1, v1), (_, v2)) => (k1, v1 + v2)
    }
  }

  /** Non-parallel key-counter */
  def keyCounter[A](zs: Seq[A]): HashMap[A, Int] = {
    val z: Seq[(A, Int)] = zs.groupBy(x => x).mapValues(x => x.length).toSeq
    HashMap[A, Int](z: _*)
  }

  /**
   * A streaming key counter
   *
   * Processing pipeline
   *  - Input <x1, x2, x3, ...>
   *  - batched inputs <[ x:s of size `batchSize`], [ x:s of size `batchSize`], ...>
   *  - process (count x:s) batches in parallel
   *  - combine and return { x1: count of x1, x2: count of x2:, ... }
   *
   * Note: performance is sensitive to the parallelization batch size.
   *
   * TODO: Check best value of batch size. Too small or too large will decrease
   * performance. This will likely depend on the number of cores, too.
   *
   */
  def keyCounter[A, B](xs: Iterator[A],
                       extractKeys: A => Seq[B],
                       batchSize: Int = 100000): HashMap[B, Int] = {

    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0")

    def counter(zs: Seq[A]): HashMap[B, Int] = keyCounter(zs.flatMap(extractKeys))

    val worker = Flow.fromFunction[Seq[A], HashMap[B, Int]](counter)
    val batchIterator: Iterator[Seq[A]] = xs.grouped(batchSize)

    val logicalCores: Int = Runtime.getRuntime.availableProcessors
    val source: Source[HashMap[B, Int], NotUsed] =
      Source.fromIterator(() => batchIterator).via(WorkBalancer.balancer(worker, logicalCores))

    var currentBatchNr = 0
    val x0 = HashMap.empty[B, Int]

    def f(acc: HashMap[B, Int], x: HashMap[B, Int]): HashMap[B, Int] = {
      currentBatchNr += 1
      println(s" > batch $currentBatchNr: x = ${acc.keySet.size}, y = ${x.keySet.size}")
      val res = StreamKeyCounter.mergeMaps(acc, x) // not parallel
      res
    }

    val sink: Sink[HashMap[B, Int], Future[HashMap[B, Int]]] = Sink.fold(x0)(f)
    Await.result(source.runWith(sink), Duration.Inf)
  }

}

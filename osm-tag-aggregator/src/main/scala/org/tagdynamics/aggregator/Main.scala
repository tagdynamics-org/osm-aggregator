package org.tagdynamics.aggregator

import org.tagdynamics.aggregator.aggregators._

import scala.io.Source

object Main {
  def process(infilename: String, outfilename: String, p: Aggregator): Unit = {
    println(s" *** OSM Aggregator (task = $p) ***")
    println(s" - Input file  : $infilename")
    println(s" - Output file : $outfilename")

    val source = Source.fromFile(infilename, "utf8")
    val parsedIt: Iterator[EntryHistory] = source.getLines().map(EntryHistory.deserialize)
    val res: Seq[p.OutputLine] = p.apply(parsedIt)
    source.close()

    println(s" - Result with ${res.length} lines ...")
    Utils.writeFile(outfilename, res, p.toJson)
    println(" - Wrote output. Done")
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new Exception("Three command line parameters should be given")
    }

    val processor: Aggregator = args(0) match {
      case "LIVE_REVCOUNTS" => LiveRevisionAggregator
      case "TOTAL_REVCOUNTS" => TotalRevisionAggregator
      case "TRANSITION_COUNTS" => TransitionsAggregator
      case "PER_DAY_DELTA_COUNTS" => DeltasByDayAggregator
      case p => throw new Exception(s"Unknown processor: $p")
    }
    val inputFile = args(1)
    val outputFile = args(2)

    process(inputFile, outputFile, processor)
  }

}

package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.BytesHelper._

/**
  * Represents a statistical snapshot of processing activity
  * @param totalRecords    the total number of records inserted
  * @param bytesRead        the current number of bytes retrieved
  * @param bytesPerSecond   the snapshot's bytes/second transfer rate
  * @param failures         the number of total failures
  * @param recordsDelta     the number of records inserted during this snapshot
  * @param recordsPerSecond the snapshot's records/second transfer rate
  * @param pctComplete      the percentage of completion
  * @param completionTime   the estimated completion time
  */
case class Statistics(totalRecords: Long,
                      bytesRead: Long,
                      bytesPerSecond: Double,
                      failures: Long,
                      failuresPerSecond: Double,
                      recordsDelta: Long,
                      recordsPerSecond: Double,
                      pctComplete: Option[Double],
                      completionTime: Option[Double]) {

  override def toString: String = {
    // generate the estimate complete time
    val pct = pctComplete.map(p => f"$p%.1f%%")
    val etc = completionTime.map(t => f"${t / 60}%.0f hrs, ${t % 60}%.0f mins")
    val completion = if (pct.isEmpty && etc.isEmpty) "" else s" (${pct.getOrElse("N/A")} - ${etc.getOrElse("N/A")})"

    // return the statistics
    f"$totalRecords records$completion, $failures failures, $recordsDelta batch ($recordsPerSecond%.1f records/sec, ${bytesPerSecond.bps})"
  }

}

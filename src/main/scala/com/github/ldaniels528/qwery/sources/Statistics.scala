package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.BytesHelper._

/**
  * Represents a statistical snapshot of processing activity
  * @param totalInserted    the total number of records inserted
  * @param bytesRead        the current number of bytes retrieved
  * @param bytesPerSecond   the snapshot's bytes/second transfer rate
  * @param failures         the number of total failures
  * @param recordsDelta     the number of records inserted during this snapshot
  * @param recordsPerSecond the snapshot's records/second transfer rate
  * @param pctComplete      the percentage of completion
  * @param completionTime   the estimated completion time
  */
class Statistics(val totalInserted: Long,
                 val bytesRead: Long,
                 val bytesPerSecond: Double,
                 val failures: Long,
                 val recordsDelta: Long,
                 val recordsPerSecond: Double,
                 val pctComplete: Double,
                 val completionTime: Double) {

  override def toString: String = {
    // generate the estimate complete time
    val etc = s"${completionTime / 60} hrs ${ completionTime % 60} mins"

    // return the statistics
    f"$totalInserted total ($pctComplete%.1f%% - $etc), failures $failures, $recordsDelta batch " +
      f"($recordsPerSecond%.1f records/sec, ${bytesPerSecond.bps})"
  }

}

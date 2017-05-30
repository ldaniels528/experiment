package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.DurationHelper._

import scala.concurrent.duration._

/**
  * Statistics Generator
  * @author lawrence.daniels@gmail.com
  */
class StatisticsGenerator(var fileSize: Option[Long] = None) {
  // totals
  private var totalRecords: Long = _
  private var totalBytesRead: Long = _
  private var totalFailures: Long = _
  private var startTime: Long = _

  // snapshots
  private var batchRecords: Long = _
  private var batchFailures: Long = _
  private var lastUpdatedTime: Long = _

  /**
    * Resets the statistics
    */
  def reset(): Unit = {
    totalRecords = 0L
    totalBytesRead = 0L
    totalFailures = 0L
    startTime = 0L
    lastUpdatedTime = 0L
  }

  /**
    * Updates the statistics
    * @param records   the number of records for the current update
    * @param bytesRead the number of bytes read for the current update
    * @param failures  the number of failures for the current update
    * @return the option of a [[Statistics]]
    */
  def update(records: Long = 0, bytesRead: Int = 0, failures: Int = 0, force: Boolean = false): Option[Statistics] = {
    if (startTime == 0) startTime = System.currentTimeMillis()

    // update the totals
    totalRecords += records
    totalBytesRead += bytesRead
    totalFailures += failures

    // has a second elapsed?
    val now = System.currentTimeMillis()
    val diff = (now - lastUpdatedTime).toDouble
    if (diff >= 10.seconds || force) {
      // compute the rates
      val totalElapsedTime = (System.currentTimeMillis() - startTime) / 1000d + 0.0001
      val batchElapsedTime = (System.currentTimeMillis() - lastUpdatedTime) / 1000d + 0.0001
      val bytesPerSecond = totalBytesRead / totalElapsedTime
      // capture the statistics
      val statistics = Some(Statistics(
        totalRecords = totalRecords,
        recordsDelta = totalRecords - batchRecords,
        recordsPerSecond = if (force) totalRecords / totalElapsedTime else (totalRecords - batchRecords) / batchElapsedTime,
        bytesRead = totalBytesRead,
        bytesPerSecond = bytesPerSecond,
        failures = totalFailures,
        failuresPerSecond = if (force) totalFailures / totalElapsedTime else (totalFailures - batchFailures) / batchElapsedTime,
        pctComplete = fileSize.map(100 * totalBytesRead / _.toDouble),
        completionTime = fileSize.map(fs => (fs - totalBytesRead) / bytesPerSecond)))

      // reset the counters
      batchRecords = totalRecords
      batchFailures = totalFailures
      lastUpdatedTime = now
      statistics
    }
    else None
  }

}

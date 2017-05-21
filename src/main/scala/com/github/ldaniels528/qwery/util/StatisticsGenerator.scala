package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.sources.Statistics

/**
  * ETL Statistics Generator
  * @param sourceFileSize the size of the source file in bytes
  */
class StatisticsGenerator(var sourceFileSize: Double = 0) {
  private var processStartTime = System.currentTimeMillis()
  private var bytesPerSecond = 0.0
  private var lastBytesRead = 0L
  private var lastRecordedCount = 0L
  private var lastUpdatedTime = System.currentTimeMillis()
  private var recordsPerSecond = 0.0

  var bytesRead = 0L
  var failures = 0L
  var totalRead = 0L
  var lastStats: Option[Statistics] = None

  /**
    * Sets the process start time (if delayed since creation of this instance)
    */
  def start(): Unit = processStartTime = System.currentTimeMillis()

  /**
    * Returns updated statistics every x frequency or when forced is true
    * @return an option of the [[Statistics]]
    */
  def update(): Statistics = {
    // compute the statistics
    val timeDelta = Math.max(System.currentTimeMillis() - lastUpdatedTime, 1.0)
    val timeDeltaSeconds = timeDelta / 1000
    lastUpdatedTime = System.currentTimeMillis()

    // records inserted
    val recordsDelta = totalRead - lastRecordedCount
    recordsPerSecond = recordsDelta / timeDeltaSeconds
    lastRecordedCount = totalRead

    // bytes read
    val bytesDelta = bytesRead - lastBytesRead
    bytesPerSecond = bytesDelta / timeDeltaSeconds
    lastBytesRead = bytesRead

    // estimated time of completion
    val complete_% = if (sourceFileSize == 0) 0 else (bytesRead / sourceFileSize) * 100.0
    val etc = {
      val avgBps = bytesRead / ((System.currentTimeMillis() - processStartTime) / 1000)
      (sourceFileSize - bytesRead) / avgBps
    }

    new Statistics(
      totalInserted = totalRead,
      bytesRead = bytesRead,
      bytesPerSecond = bytesPerSecond,
      failures = failures,
      recordsDelta = recordsDelta,
      recordsPerSecond = recordsPerSecond,
      pctComplete = complete_%,
      completionTime = etc)
  }

}


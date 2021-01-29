package com.qwery.database.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.qwery.database.clients.MessageProducer
import com.qwery.database.kinesis.RecordProcessorFactory.RecordProcessor
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
 * Qwery Record Processor Factory
 * @param host         the remote hostname
 * @param port         the remote port
 * @param databaseName the database name
 * @param tableName    the table name
 */
class RecordProcessorFactory(host: String, port: Int, databaseName: String, tableName: String) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    new RecordProcessor(host, port, databaseName, tableName)
  }

}

/**
 * Record Processor Factory Companion
 */
object RecordProcessorFactory {
  private var lastRecords = 0L
  private var lastUpdateTime = System.currentTimeMillis()
  private val records = new AtomicLong(0L)

  /**
   * Qwery Record Processor
   * @param host         the remote hostname
   * @param port         the remote port
   * @param databaseName the database name
   * @param tableName    the table name
   */
  class RecordProcessor(host: String, port: Int, databaseName: String, tableName: String) extends IRecordProcessor {
    private val logger = LoggerFactory.getLogger(getClass)
    private val producer = MessageProducer(host, port)

    override def initialize(initializationInput: InitializationInput): Unit = ()

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      val checkPointer = processRecordsInput.getCheckpointer
      val incomingRecords = processRecordsInput.getRecords.asScala.toList
      try {
        incomingRecords.foreach { record =>
          val jsonString = new String(record.getData.array())
          try producer.send(databaseName, tableName, jsonString) catch {
            case e: Exception =>
              logger.error(s"${e.getMessage}: $jsonString")
          }
          records.addAndGet(1)
        }

        // compute the statistics
        val diff = (System.currentTimeMillis() - lastUpdateTime) / 1000.0
        if (diff >= 30) {
          val delta = records.get - lastRecords
          val rps = delta / diff.toDouble
          logger.info(f"total: ${records.get} | delta: $delta | records/sec: $rps%.1f")
          lastUpdateTime = System.currentTimeMillis()
          lastRecords = 0
        }
      } finally {
        checkPointer.checkpoint()
      }
    }

    override def shutdown(shutdownInput: ShutdownInput): Unit = ()

  }

}
package com.qwery.database.awstools.kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import org.slf4j.LoggerFactory

import java.util.UUID

/**
 * Qwery Kinesis Sync
 */
object KinesisSync {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Starts the application
   * @param args the commandline arguments
   */
  def main(args: Array[String]): Unit = {
    logger.info("starting Kinesis Writer...")

    // get the config properties
    logger.info("Loading the configuration...")
    val config = loadConfig(args)

    // start the application
    start(config)
  }

  def start(config: KinesisSyncConfig): Unit = {
    // create the Kinesis client
    logger.info("Initializing the Kinesis client...")
    val credentialsProvider = new DefaultAWSCredentialsProviderChain()
    val kinesisClient = AmazonKinesisClientBuilder.standard()
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withRegion(config.region)
      .build()

    val workerId = UUID.randomUUID().toString
    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(config.applicationName, config.streamName, credentialsProvider, workerId)
      .withRegionName(config.region)
      .withInitialPositionInStream(InitialPositionInStream.LATEST)
      .withInitialLeaseTableReadCapacity(5)
      .withInitialLeaseTableWriteCapacity(5)
      .withCallProcessRecordsEvenForEmptyRecordList(true)

    // create the worker
    logger.info("Initializing the worker...")
    val worker = new Worker.Builder()
      .recordProcessorFactory(new RecordProcessorFactory(config.host, config.port, config.databaseName, config.tableName))
      .kinesisClient(kinesisClient)
      .config(kinesisClientLibConfiguration)
      .build()

    // start the worker
    logger.info("Starting the worker...")
    worker.run()
  }

  private def loadConfig(args: Array[String]): KinesisSyncConfig = {
    // get the config properties
    args match {
      case Array(host, port, databaseName, tableName, applicationName, streamName, region) =>
        KinesisSyncConfig(host, port.toInt, databaseName, tableName, applicationName, streamName, region)
      case _ => throw new IllegalArgumentException(s"java ${getClass.getName.replaceAllLiterally("$", "")} <host> <port> <databaseName> <tableName> <applicationName> <streamName>")
    }
  }

  case class KinesisSyncConfig(host: String,
                               port: Int,
                               databaseName: String,
                               tableName: String,
                               applicationName: String,
                               streamName: String,
                               region: String)

}

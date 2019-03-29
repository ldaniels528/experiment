package com.qwery
package platform
package sparksql

import com.qwery.models.{Table, TableLike}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}
import org.slf4j.LoggerFactory

/**
  * Spark Helper
  * @author lawrence.daniels@gmail.com
  */
object SparkHelper {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * DataFrame Reader Enrichment
    * @param dataFrameReader the given [[DataFrameReader]]
    */
  final implicit class DataFrameReaderEnriched(val dataFrameReader: DataFrameReader) extends AnyVal {
    @inline def tableOptions(tableLike: TableLike): DataFrameReader = {
      var dfr: DataFrameReader = dataFrameReader
      tableLike match {
        case table: Table =>
          table.fieldDelimiter.foreach(delimiter => dfr = dfr.option("delimiter", delimiter))
          table.headersIncluded.foreach(enabled => dfr = dfr.option("header", enabled.toString))
          table.nullValue.foreach(value => dfr = dfr.option("nullValue", value))
          for ((key, value) <- table.properties ++ table.serdeProperties) dfr.option(key, value)
          logger.info(s"Table ${table.name} read options: ${table.properties}")
        case table =>
          logger.warn(s"Unhandled table entity '${table.name}' for reading")
      }
      dfr
    }
  }

  /**
    * DataFrame Writer Enrichment
    * @param dataFrameWriter the given [[DataFrameWriter]]
    */
  final implicit class DataFrameWriterEnriched[T](val dataFrameWriter: DataFrameWriter[T]) extends AnyVal {
    @inline def tableOptions(tableLike: TableLike): DataFrameWriter[T] = {
      var dfw: DataFrameWriter[T] = dataFrameWriter
      tableLike match {
        case table: Table =>
          table.fieldDelimiter.foreach(delimiter => dfw = dfw.option("delimiter", delimiter))
          table.headersIncluded.foreach(enabled => dfw = dfw.option("header", enabled.toString))
          table.nullValue.foreach(value => dfw = dfw.option("nullValue", value))
          for ((key, value) <- table.properties ++ table.serdeProperties) dfw.option(key, value)
          logger.info(s"Table ${table.name} write options: ${table.properties}")
        case table =>
          logger.warn(s"Unhandled table entity '${table.name}' for writing")
      }
      dfw
    }
  }

}

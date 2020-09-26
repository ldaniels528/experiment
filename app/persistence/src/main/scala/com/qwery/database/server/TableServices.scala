package com.qwery.database.server

import com.qwery.database.ROWID
import com.qwery.database.server.TableServices.{TableColumn, TableStatistics}
import org.slf4j.LoggerFactory

/**
 * Table Services
 */
trait TableServices {
  private val logger = LoggerFactory.getLogger(getClass)

  def deleteRow(tableName: String, rowID: ROWID)(implicit tableManager: TableManager): Boolean = {
    val (_, responseTime) = time(tableManager(tableName).delete(rowID))
    logger.info(f"$tableName($rowID) ~> deleted [in $responseTime%.1f msec]")
    true
  }

  def deleteRows(tableName: String, condition: Map[Symbol, Any], limit: Option[Int] = None)(implicit tableManager: TableManager): Boolean = {
    val (count, responseTime) = time(tableManager(tableName).delete(condition, limit))
    logger.info(f"$tableName($condition) ~> $count [in $responseTime%.1f msec]")
    true
  }

  def getRow(tableName: String, rowID: ROWID)(implicit tableManager: TableManager): Map[String, Any] = {
    val (row, responseTime) = time(Map(tableManager(tableName).get(rowID): _*))
    logger.info(f"$tableName($rowID) ~> $row [in $responseTime%.1f msec]")
    row
  }

  def getRows(tableName: String, start: ROWID, length: ROWID)(implicit tableManager: TableManager): Seq[Map[String, Any]] = {
    val (rows, responseTime) = time(tableManager(tableName).slice(start, length))
    logger.info(f"$tableName($start, $length) ~> [..] [in $responseTime%.1f msec]")
    rows.map(s => Map(s: _*))
  }

  def getStatistics(tableName: String)(implicit tableManager: TableManager): TableStatistics = {
    val (stats, responseTime) = time {
      val table = tableManager(tableName)
      val device = table.device
      TableStatistics(
        name = table.name,
        physicalSize = device.getPhysicalSize,
        recordSize = device.recordSize,
        rowCount = table.count(),
        columns = device.columns.toList.map(c =>
          TableColumn(
            name = c.name,
            `type` = c.metadata.`type`.toString,
            maxLength = if (c.metadata.`type`.isFixedLength) None else Some(c.maxLength - 3),
            isCompressed = c.metadata.isCompressed,
            isEncrypted = c.metadata.isEncrypted,
            isNullable = c.metadata.isNullable,
            isPrimary = c.metadata.isPrimary,
            isRowID = c.metadata.isRowID,
          )))
    }
    logger.info(f"$tableName.statistics ~> $stats [in $responseTime%.1f msec]")
    stats
  }

  def find(tableName: String,
           condition: Map[Symbol, Any],
           limit: Option[Int] = None)(implicit tableManager: TableManager): Seq[Map[String, Any]] = {
    val (rows, responseTime) = time(tableManager(tableName).find(condition, limit))
    logger.info(f"$tableName($condition) ~> [..] [in $responseTime%.1f msec]")
    rows.map(s => Map(s: _*))
  }

  private def time[A](block: => A): (A, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val elapsedTime = (System.nanoTime() - startTime) / 1e+6
    (result, elapsedTime)
  }

}

/**
 * Table Services Companion
 */
object TableServices {

  def apply(): TableServices = new TableServices {}

  case class TableStatistics(name: String,
                             columns: List[TableColumn],
                             physicalSize: Option[Long],
                             recordSize: Int,
                             rowCount: ROWID)

  case class TableColumn(name: String,
                         `type`: String,
                         maxLength: Option[Int],
                         isCompressed: Boolean,
                         isEncrypted: Boolean,
                         isNullable: Boolean,
                         isPrimary: Boolean,
                         isRowID: Boolean)

}

package com.qwery.database.server

import com.qwery.database.QweryFiles._
import com.qwery.database.server.InvokableProcessor.implicits._
import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.TableService.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.server.TableService._
import com.qwery.database.{ROWID, Row}
import com.qwery.language.SQLLanguageParser
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Server-Side Table Service
 */
case class ServerSideTableService() extends TableService[Row] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tables = TrieMap[(String, String), TableFile]()

  def apply(databaseName: String, tableName: String): TableFile = {
    tables.getOrElseUpdate(databaseName -> tableName, TableFile(databaseName, tableName))
  }

  override def appendRow(databaseName: String, tableName: String, values: TupleSet): QueryResult = {
    logger.info(s"$databaseName.$tableName <~ $values")
    val (rowID, responseTime) = time(apply(databaseName, tableName).insertRow(values))
    QueryResult(databaseName, tableName, count = 1, responseTime=responseTime, __id = Some(rowID))
  }

  override def createTable(databaseName: String, ref: TableCreation): QueryResult = {
    val (_, responseTime) = time(TableFile.createTable(databaseName, ref.tableName, ref.columns.map(_.toColumn)))
    QueryResult(databaseName, ref.tableName, count = 1, responseTime=responseTime)
  }

  override def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): QueryResult = {
    val (isDeleted, responseTime) = time(apply(databaseName, tableName).deleteField(rowID, columnID))
    QueryResult(databaseName, tableName, count = if (isDeleted) 1 else 0, responseTime = responseTime)
  }

  override def deleteRange(databaseName: String, tableName: String, start: ROWID, length: Int): QueryResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).deleteRange(start, length))
    logger.info(f"$tableName($start..${length + start}) ~> deleted $count items [in $responseTime%.1f msec]")
    QueryResult(databaseName, tableName, count = count, responseTime = responseTime)
  }

  override def deleteRow(databaseName: String, tableName: String, rowID: ROWID): QueryResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).deleteRow(rowID))
    logger.info(f"$tableName($rowID) ~> deleted $count items [in $responseTime%.1f msec]")
    QueryResult(databaseName, tableName, count = count, responseTime = responseTime, __id = Some(rowID))
  }

  override def dropTable(databaseName: String, tableName: String): QueryResult = {
    tables.remove(databaseName -> tableName) foreach (_.close())
    val (isDropped, responseTime) = time(TableFile.dropTable(databaseName, tableName))
    QueryResult(databaseName, tableName, count = if (isDropped) 1 else 0, responseTime = responseTime)
  }

  override def executeQuery(databaseName: String, sql: String): QueryResult = {
    val (rows, responseTime) = time(SQLLanguageParser.parse(sql).invoke(databaseName)(this))
    logger.info(f"$sql ~> $rows [in $responseTime%.1f msec]")
    rows
  }

  override def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[Row] = {
    val (rows, responseTime) = time(apply(databaseName, tableName).executeQuery(condition, limit))
    logger.info(f"$tableName($condition) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getDatabaseMetrics(databaseName: String): DatabaseMetrics = {
    val (metrics, responseTime) = time {
      val directory = getDatabaseRootDirectory(databaseName)
      val tableConfigs = directory.listFilesRecursively.map(_.getName) flatMap {
        case name if name.endsWith(".json") =>
          name.lastIndexOf('.') match {
            case -1 => None
            case index => Some(name.substring(0, index))
          }
        case _ => None
      }
      DatabaseMetrics(databaseName = databaseName, tables = tableConfigs)
    }
    logger.info(f"$databaseName.metrics ~> ${metrics.toJSON} [in $responseTime%.1f msec]")
    metrics.copy(responseTimeMillis = responseTime)
  }

  override def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte] = {
    val tableFile = apply(databaseName, tableName)
    val (field, responseTime) = time(tableFile.getField(rowID, columnID))
    val column = tableFile.device.columns(columnID)
    logger.info(f"$tableName($rowID, $columnID:${column.name}) ~> '${field.value}' [in $responseTime%.1f msec]")
    field.typedValue.encode(column)
  }

  override def getLength(databaseName: String, tableName: String): QueryResult = {
    val (length, responseTime) = time(apply(databaseName, tableName).device.length)
    QueryResult(databaseName, tableName, responseTime = responseTime, __id = Some(length))
  }

  override def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[Row] = {
    val (rows, responseTime) = time(apply(databaseName, tableName).getRange(start, length))
    logger.info(f"$tableName($start, $length) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[Row] = {
    val (row, responseTime) = time(apply(databaseName, tableName).get(rowID))
    logger.info(f"$tableName($rowID) ~> ${row.map(_.toMap).orNull} [in $responseTime%.1f msec]")
    row
  }

  override def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    val (metrics, responseTime) = time {
      val table = apply(databaseName, tableName)
      val device = table.device
      TableMetrics(
        databaseName = databaseName, tableName = table.tableName, columns = device.columns.toList.map(_.toTableColumn),
        physicalSize = device.getPhysicalSize, recordSize = device.recordSize, rows = device.length)
    }
    logger.info(f"$tableName.metrics ~> ${metrics.toJSON} [in $responseTime%.1f msec]")
    metrics.copy(responseTimeMillis = responseTime)
  }

  override def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult = {
    val (_, responseTime) = time(apply(databaseName, tableName).replaceRow(rowID, values))
    logger.info(f"$tableName($rowID) ~> $values [in $responseTime%.1f msec]")
    QueryResult(databaseName, tableName, count = 1, responseTime = responseTime)
  }

  override def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): QueryResult = {
    val (_, responseTime) = time(apply(databaseName, tableName).updateField(rowID, columnID, value))
    logger.info(f"$tableName($rowID, $columnID) <~ $value [in $responseTime%.1f msec]")
    QueryResult(databaseName, tableName, count = 1, responseTime = responseTime)
  }

  override def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult = {
    val (newValues, responseTime) = time {
      val tableFile = apply(databaseName, tableName)
      for {
        oldValues <- tableFile.get(rowID).map(_.toMap)
        newValues = oldValues ++ values
      } yield {
        tableFile.replaceRow(rowID, newValues)
        newValues
      }
    }
    logger.info(f"$tableName($rowID) ~> ${newValues.orNull} [in $responseTime%.1f msec]")
    QueryResult(databaseName, tableName, count = newValues.map(_ => 1).getOrElse(0), responseTime = responseTime, __id = Some(rowID))
  }

}
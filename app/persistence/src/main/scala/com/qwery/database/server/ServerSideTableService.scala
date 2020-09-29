package com.qwery.database.server

import java.io.File

import com.qwery.database.server.ServerSideTableService.InvokableFacade
import com.qwery.database.server.TableFile.{ColumnToTableColumnConversion, getDataDirectory}
import com.qwery.database.server.TableService.UpdateResult
import com.qwery.database.{BlockDevice, Column, ColumnMetadata, ColumnTypes, ROWID}
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Insert.Into
import com.qwery.models.expressions._
import com.qwery.models.{ColumnTypes => QwColumnTypes, _}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Server-Side Table Service
 */
case class ServerSideTableService() extends TableService[TupleSet] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tables = TrieMap[String, TableFile]()

  def apply(tableName: String): TableFile = tables.getOrElseUpdate(tableName, TableFile(tableName))

  override def appendRow(tableName: String, row: TupleSet): UpdateResult = {
    logger.info(s"row => $row")
    val (rowID, responseTime) = time(apply(tableName).insert(row))
    UpdateResult(count = 1, responseTime, rowID = Some(rowID))
  }

  override def deleteRow(tableName: String, rowID: ROWID): UpdateResult = {
    val (_, responseTime) = time(apply(tableName).delete(rowID))
    logger.info(f"$tableName($rowID) ~> deleted [in $responseTime%.1f msec]")
    UpdateResult(count = 1, responseTime)
  }

  override def dropTable(tableName: String): UpdateResult = {
    val (isDropped, responseTime) = time(tables.remove(tableName) exists { table =>
      table.close()
      table.drop()
    })
    UpdateResult(count = if(isDropped) 1 else 0, responseTime)
  }

  override def executeQuery(sql: String): Seq[TupleSet] = {
    val (rows, responseTime) = time(SQLLanguageParser.parse(sql).invoke(this))
    logger.info(f"$sql ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def findRows(tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[TupleSet] = {
    val (rows, responseTime) = time(apply(tableName).findRows(condition, limit))
    logger.info(f"$tableName($condition) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getRow(tableName: String, rowID: ROWID): Option[TupleSet] = {
    val (row, responseTime) = time(apply(tableName).get(rowID))
    logger.info(f"$tableName($rowID) ~> $row [in $responseTime%.1f msec]")
    if(row.nonEmpty) Some(row) else None
  }

  override def getRows(tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    val (rows, responseTime) = time(apply(tableName).slice(start, length))
    logger.info(f"$tableName($start, $length) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getStatistics(tableName: String): TableFile.TableStatistics = {
    val (stats, responseTime) = time {
      val table = apply(tableName)
      val device = table.device
      TableFile.TableStatistics(name = table.tableName, columns = device.columns.toList.map(_.toTableColumn),
        physicalSize = device.getPhysicalSize, recordSize = device.recordSize, rows = device.length,
        responseTimeMillis = 0)
    }
    logger.info(f"$tableName.statistics ~> $stats [in $responseTime%.1f msec]")
    stats.copy(responseTimeMillis = responseTime)
  }

}

/**
 * Table Service Companion
 */
object ServerSideTableService {
  private val columnTypeMap = Map(
    QwColumnTypes.ARRAY -> ColumnTypes.ArrayType,
    QwColumnTypes.BINARY -> ColumnTypes.BlobType,
    QwColumnTypes.BOOLEAN -> ColumnTypes.BooleanType,
    QwColumnTypes.DATE -> ColumnTypes.DateType,
    QwColumnTypes.DOUBLE -> ColumnTypes.DoubleType,
    QwColumnTypes.FLOAT -> ColumnTypes.FloatType,
    QwColumnTypes.INTEGER -> ColumnTypes.IntType,
    QwColumnTypes.LONG -> ColumnTypes.LongType,
    QwColumnTypes.SHORT -> ColumnTypes.ShortType,
    QwColumnTypes.STRING -> ColumnTypes.StringType,
    QwColumnTypes.TIMESTAMP -> ColumnTypes.DateType,
    QwColumnTypes.UUID -> ColumnTypes.UUIDType
  )

  def createTable(t: Table): TableFile = {
    TableFile.create(name = t.name, columns = t.columns.map { c =>
      Column(
        name = c.name,
        comment = c.comment.getOrElse(""),
        maxSize = c.`type` match { // TODO this is a stop-gap
          case QwColumnTypes.BINARY => Some(8192)
          case QwColumnTypes.STRING => Some(255)
          case _ => None
        },
        metadata = ColumnMetadata(
          isNullable = c.isNullable,
          `type` = columnTypeMap.getOrElse(c.`type`, ColumnTypes.BlobType)
        ))
    })
  }

  def createTableIndex(indexName: String, location: Location, indexColumns: Seq[Field])
                      (implicit tables: ServerSideTableService): Option[BlockDevice] = {
    location match {
      case TableRef(tableName) =>
        val table = tables(tableName)
        val device = table.device
        for {
          indexColumnName <- indexColumns.headOption.map(_.name)
          indexColumn <- device.columns.find(_.name == indexColumnName)
        } yield table.createIndex(indexName, indexColumn)
      case unknown => throw new IllegalArgumentException(s"Unsupported location type $unknown")
    }
  }

  def findTables(theFile: File = getDataDirectory): List[File] = theFile match {
    case directory if directory.isDirectory => directory.listFiles().toList.flatMap(findTables)
    case file if file.getName.endsWith(".qdb") => file :: Nil
    case _ => Nil
  }

  def insertRows(tableName: String, fields: Seq[String], rowValuesList: List[List[Any]])(implicit tables: ServerSideTableService): Seq[TupleSet] = {
    val table = tables(tableName)
    for {
      rowValues <- rowValuesList
      rowID = table.insert(values = Map(fields zip rowValues: _*))
    } yield Map("rowID" -> rowID)
  }

  def selectRows(select: Select)(implicit tables: ServerSideTableService): Seq[TupleSet] = {
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = tables(tableName)
        val conditions: TupleSet = select.where match {
          case Some(ConditionalOp(Field(name), Literal(value), "==", "=")) => Map(name -> value)
          case Some(condition) =>
            throw new IllegalArgumentException(s"Unsupported condition $condition")
          case None => Map.empty
        }
        table.findRows(conditions, limit = select.limit)  // TODO select.fields & select.orderBy
      case Some(queryable) =>
        throw new IllegalArgumentException(s"Unsupported queryable $queryable")
      case None => Nil
    }
  }

  final implicit class ExpressionFacade(val expression: Expression) extends AnyVal {
    def translate: Any = expression match {
      case Literal(value) => value
      case unknown => throw new IllegalArgumentException(s"Unsupported value $unknown")
    }
  }

  final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
    def invoke(implicit tables: ServerSideTableService): Seq[TupleSet] = invokable match {
      case Create(table: Table) => createTable(table); Nil
      case Create(TableIndex(name, table, columns)) => createTableIndex(name, table, columns); Nil
      case Insert(Into(TableRef(name)), Insert.Values(expressionValues), fields) =>
        insertRows(name, fields.map(_.name), expressionValues.map(_.map(_.translate)))
      case select: Select => selectRows(select)
      case Truncate(TableRef(name)) => tables(name).truncate(); Nil
      case unknown => throw new IllegalArgumentException(s"Unsupported operation $unknown")
    }
  }

}
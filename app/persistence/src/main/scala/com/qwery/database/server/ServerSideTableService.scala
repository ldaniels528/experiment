package com.qwery.database.server

import com.qwery.database.server.JSONSupport.JSONProduct
import com.qwery.database.server.ServerSideTableService.InvokableFacade
import com.qwery.database.server.TableFile._
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
  private val tables = TrieMap[(String, String), TableFile]()

  def apply(databaseName: String, tableName: String): TableFile = {
    tables.getOrElseUpdate(databaseName -> tableName, TableFile(databaseName, tableName))
  }

  override def appendRow(databaseName: String, tableName: String, row: TupleSet): UpdateResult = {
    logger.info(s"$databaseName.$tableName <~ $row")
    val (rowID, responseTime) = time(apply(databaseName, tableName).insert(row))
    UpdateResult(count = 1, responseTime, rowID = Some(rowID))
  }

  override def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).deleteRange(start, length))
    logger.info(f"$tableName($start..${length + start}) ~> deleted $count [in $responseTime%.1f msec]")
    UpdateResult(count, responseTime)
  }

  override def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).delete(rowID))
    logger.info(f"$tableName($rowID) ~> deleted $count [in $responseTime%.1f msec]")
    UpdateResult(count, responseTime)
  }

  override def dropTable(databaseName: String, tableName: String): UpdateResult = {
    tables.remove(databaseName -> tableName) foreach(_.close())
    val (isDropped, responseTime) = time(TableFile.dropTable(databaseName, tableName))
    UpdateResult(count = if(isDropped) 1 else 0, responseTime)
  }

  override def executeQuery(databaseName: String, sql: String): Seq[TupleSet] = {
    val (rows, responseTime) = time(SQLLanguageParser.parse(sql).invoke(this))
    logger.info(f"$sql ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[TupleSet] = {
    val (rows, responseTime) = time(apply(databaseName, tableName).findRows(condition, limit))
    logger.info(f"$tableName($condition) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getDatabaseMetrics(databaseName: String): TableFile.DatabaseMetrics = {
    val (stats, responseTime) = time {
      val directory = getDatabaseRootDirectory(databaseName)
      val tableConfigs = directory.listFilesRecursively.map(_.getName) flatMap {
        case name if name.endsWith(".json") =>
          name.lastIndexOf('.') match {
            case -1 => None
            case index => Some(name.substring(0, index))
          }
        case _ => None
      }
      TableFile.DatabaseMetrics(databaseName = databaseName, tables = tableConfigs)
    }
    logger.info(f"$databaseName.metrics ~> ${stats.toJSON} [in $responseTime%.1f msec]")
    stats.copy(responseTimeMillis = responseTime)
  }

  override def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    val (rows, responseTime) = time(apply(databaseName, tableName).getRange(start, length))
    logger.info(f"$tableName($start, $length) ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[TupleSet] = {
    val (row, responseTime) = time(apply(databaseName, tableName).get(rowID))
    logger.info(f"$tableName($rowID) ~> $row [in $responseTime%.1f msec]")
    if (row.nonEmpty) Some(row) else None
  }

  override def getTableMetrics(databaseName: String, tableName: String): TableFile.TableMetrics = {
    val (stats, responseTime) = time {
      val table = apply(databaseName, tableName)
      val device = table.device
      TableFile.TableMetrics(
        databaseName = databaseName, tableName = table.tableName, columns = device.columns.toList.map(_.toTableColumn),
        physicalSize = device.getPhysicalSize, recordSize = device.recordSize, rows = device.length)
    }
    logger.info(f"$tableName.metrics ~> ${stats.toJSON} [in $responseTime%.1f msec]")
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
    TableFile.createTable(databaseName = DEFAULT_DATABASE, tableName = t.name, columns = t.columns.map { c =>
      Column(
        name = c.name,
        comment = c.comment.getOrElse(""),
        maxSize = c.precision.headOption,
        metadata = ColumnMetadata(
          isNullable = c.isNullable,
          `type` = columnTypeMap.getOrElse(c.`type`, ColumnTypes.BlobType)
        ))
    })
  }

  def createTableIndex(databaseName: String, indexName: String, location: Location, indexColumns: Seq[Field])
                      (implicit tables: ServerSideTableService): Option[BlockDevice] = {
    location match {
      case TableRef(tableName) =>
        val table = tables(databaseName, tableName)
        val device = table.device
        for {
          indexColumnName <- indexColumns.headOption.map(_.name)
          indexColumn <- device.columns.find(_.name == indexColumnName)
        } yield table.createIndex(indexName, indexColumn)
      case unknown => throw new IllegalArgumentException(s"Unsupported location type $unknown")
    }
  }

  def insertRows(tableName: String, fields: Seq[String], rowValuesList: List[List[Any]])(implicit tables: ServerSideTableService): Seq[TupleSet] = {
    val table = tables(DEFAULT_DATABASE, tableName)
    for {
      rowValues <- rowValuesList
      rowID = table.insert(values = Map(fields zip rowValues: _*))
    } yield Map("rowID" -> rowID)
  }

  def selectRows(select: Select)(implicit tables: ServerSideTableService): Seq[TupleSet] = {
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = tables(DEFAULT_DATABASE, tableName)
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
      case Create(TableIndex(name, table, columns)) => createTableIndex(DEFAULT_DATABASE, name, table, columns); Nil
      case Insert(Into(TableRef(name)), Insert.Values(expressionValues), fields) =>
        insertRows(name, fields.map(_.name), expressionValues.map(_.map(_.translate)))
      case select: Select => selectRows(select)
      case Truncate(TableRef(name)) => tables(DEFAULT_DATABASE, name).truncate(); Nil
      case unknown => throw new IllegalArgumentException(s"Unsupported operation $unknown")
    }
  }

}
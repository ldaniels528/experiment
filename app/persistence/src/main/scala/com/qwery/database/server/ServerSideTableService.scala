package com.qwery.database.server

import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.ServerSideTableService.InvokableFacade
import com.qwery.database.server.TableFile._
import com.qwery.database.server.TableService.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.server.TableService._
import com.qwery.database.{BlockDevice, Column, ColumnMetadata, ColumnTypes, FieldMetadata, ROWID, Row, RowMetadata}
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Insert.Into
import com.qwery.models.expressions._
import com.qwery.models.{ColumnTypes => QwColumnTypes, _}
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

  override def appendRow(databaseName: String, tableName: String, values: TupleSet): UpdateResult = {
    logger.info(s"$databaseName.$tableName <~ $values")
    val (rowID, responseTime) = time(apply(databaseName, tableName).insert(values))
    UpdateResult(count = 1, responseTime, __id = Some(rowID))
  }

  override def createTable(databaseName: String, ref: TableCreation): UpdateResult = {
    val (_, responseTime) = time(TableFile.createTable(databaseName, ref.tableName, ref.columns.map(_.toColumn)))
    UpdateResult(count = 1, responseTime)
  }

  override def deleteRange(databaseName: String, tableName: String, start: ROWID, length: Int): UpdateResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).deleteRange(start, length))
    logger.info(f"$tableName($start..${length + start}) ~> deleted $count items [in $responseTime%.1f msec]")
    UpdateResult(count, responseTime)
  }

  override def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateResult = {
    val (count, responseTime) = time(apply(databaseName, tableName).delete(rowID))
    logger.info(f"$tableName($rowID) ~> deleted $count items [in $responseTime%.1f msec]")
    UpdateResult(count, responseTime, __id = Some(rowID))
  }

  override def dropTable(databaseName: String, tableName: String): UpdateResult = {
    tables.remove(databaseName -> tableName) foreach(_.close())
    val (isDropped, responseTime) = time(TableFile.dropTable(databaseName, tableName))
    UpdateResult(count = if (isDropped) 1 else 0, responseTime)
  }

  override def executeQuery(databaseName: String, tableName: String, sql: String): Seq[Row] = {
    val (rows, responseTime) = time(SQLLanguageParser.parse(sql).invoke(databaseName)(this))
    logger.info(f"$sql ~> (${rows.length} items) [in $responseTime%.1f msec]")
    rows
  }

  override def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[Row] = {
    val (rows, responseTime) = time(apply(databaseName, tableName).findRows(condition, limit))
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

  override def getLength(databaseName: String, tableName: String): UpdateResult = {
    val (length, responseTime) = time(apply(databaseName, tableName).device.length)
    UpdateResult(count = 0, responseTime, __id = Some(length))
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

  override def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): UpdateResult = {
    val (_, responseTime) = time(apply(databaseName, tableName).replace(rowID, values))
    logger.info(f"$tableName($rowID) ~> $values [in $responseTime%.1f msec]")
    UpdateResult(count = 1, responseTime, __id = Some(rowID))
  }

  override def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): UpdateResult = {
    val (newValues, responseTime) = time {
      val tableFile = apply(databaseName, tableName)
      for {
        oldValues <- tableFile.get(rowID).map(_.toMap)
        newValues = oldValues ++ values
      } yield {
        tableFile.replace(rowID, newValues)
        newValues
      }
    }
    logger.info(f"$tableName($rowID) ~> ${newValues.orNull} [in $responseTime%.1f msec]")
    UpdateResult(count = newValues.map(_ => 1).getOrElse(0), responseTime, __id = Some(rowID))
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

  def createTable(databaseName: String, t: Table): TableFile = {
    TableFile.createTable(databaseName = databaseName, tableName = t.name, columns = t.columns.map { c =>
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
                      (implicit service: ServerSideTableService): Option[BlockDevice] = {
    location match {
      case TableRef(tableName) =>
        val table = service(databaseName, tableName)
        val device = table.device
        for {
          indexColumnName <- indexColumns.headOption.map(_.name)
          indexColumn <- device.columns.find(_.name == indexColumnName)
        } yield table.createIndex(indexName, indexColumn)
      case unknown => throw new IllegalArgumentException(s"Unsupported location type $unknown")
    }
  }

  def insertRows(databaseName: String, tableName: String, fields: Seq[String], rowValuesList: List[List[Any]])
                (implicit service: ServerSideTableService): List[ROWID] = {
    val table = service(databaseName, tableName)
    for {
      rowValues <- rowValuesList
      rowID = table.insert(values = Map(fields zip rowValues: _*))
    } yield rowID
  }

  def selectRows(databaseName: String, select: Select)(implicit service: ServerSideTableService): Seq[Row] = {
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = service(databaseName, tableName)
        val conditions: TupleSet = select.where match {
          case Some(ConditionalOp(Field(name), value, "==", "=")) => Map(name -> value.translate)
          case Some(condition) => throw new IllegalArgumentException(s"Unsupported condition $condition")
          case None => Map.empty
        }
        table.findRows(conditions, limit = select.limit) // TODO select.fields & select.orderBy
      case Some(queryable) =>
        throw new IllegalArgumentException(s"Unsupported queryable $queryable")
      case None => Nil
    }
  }

  private def createUpdateResultSet(count: Int, responseTime: Double, __id: Option[ROWID] = None): Seq[Row] = {
    import com.qwery.database.{Field => DBField}
    Seq(Row(__id.getOrElse(-1), RowMetadata(), fields = Seq(
      DBField(name = "count", FieldMetadata(`type` = ColumnTypes.IntType), value = Some(count)),
      DBField(name = "responseTimeMillis", FieldMetadata(`type` = ColumnTypes.DoubleType), value = Some(responseTime))
    )))
  }

  final implicit class ExpressionFacade(val expression: Expression) extends AnyVal {
    def translate: Any = expression match {
      case Literal(value) => value
      case unknown => throw new IllegalArgumentException(s"Unsupported value $unknown")
    }
  }

  final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
    def invoke(databaseName: String)(implicit service: ServerSideTableService): Seq[Row] = invokable match {
      case Create(table: Table) =>
        val (_, responseTime) = time(createTable(databaseName, table))
        createUpdateResultSet(count = 1, responseTime = responseTime)
      case Create(TableIndex(name, table, columns)) =>
        val (_, responseTime) = time(createTableIndex(databaseName, name, table, columns))
        createUpdateResultSet(count = 1, responseTime = responseTime)
      case DropTable(TableRef(tableName)) =>
        val (isDropped, responseTime) = time(dropTable(databaseName, tableName))
        createUpdateResultSet(count = if (isDropped) 1 else 0, responseTime)
      case Insert(Into(TableRef(tableName)), Insert.Values(expressionValues), fields) =>
        val (results, responseTime) = time(insertRows(databaseName, tableName, fields.map(_.name), expressionValues.map(_.map(_.translate))))
        results.flatMap(rowID => createUpdateResultSet(count = 1, responseTime, __id = Some(rowID)))
      case select: Select => selectRows(databaseName, select)
      case Truncate(TableRef(tableName)) =>
        val (_, responseTime) = time(service(databaseName, tableName).truncate())
        createUpdateResultSet(count = 1, responseTime = responseTime)
      case unknown => throw new IllegalArgumentException(s"Unsupported operation $unknown")
    }
  }

}
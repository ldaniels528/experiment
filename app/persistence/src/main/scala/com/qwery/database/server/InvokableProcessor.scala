package com.qwery.database.server

import com.qwery.database.server.InvokableProcessor.implicits._
import com.qwery.database.server.TableService.QueryResult
import com.qwery.database.server.TableService.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.{Column, ColumnMetadata, ColumnTypes}
import com.qwery.models.Insert.Into
import com.qwery.models.expressions._
import com.qwery.models.{ColumnTypes => QwColumnTypes, _}

/**
 * Invokable Processor
 */
object InvokableProcessor {
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

  /**
   * Creates a new table
   * @param databaseName the database name
   * @param table        the [[Table table]]
   * @return the [[QueryResult]]
   */
  def createTable(databaseName: String, table: Table): QueryResult = {
    val (_, responseTime) = time {
      TableFile.createTable(databaseName = databaseName, tableName = table.name, columns = table.columns.map { c =>
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
    QueryResult(databaseName, table.name, count = 1, responseTime = responseTime)
  }

  /**
   * Creates a new table index
   * @param databaseName the database name
   * @param indexName    the index name
   * @param location     the [[Location]]
   * @param indexColumns the index [[Field columns]]
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[QueryResult]]
   */
  def createTableIndex(databaseName: String, indexName: String, location: Location, indexColumns: Seq[Field])
                      (implicit service: ServerSideTableService): QueryResult = {
    val (_, responseTime) = time {
      location match {
        case TableRef(tableName) =>
          val table = service(databaseName, tableName)
          val device = table.device
          for {
            indexColumnName <- indexColumns.headOption.map(_.name)
            indexColumn <- device.columns.find(_.name == indexColumnName)
          } table.createIndex(indexName, indexColumn)
        case unknown => throw new IllegalArgumentException(s"Unsupported location type $unknown")
      }
    }
    QueryResult(databaseName, indexName, count = 1, responseTime = responseTime)
  }

  /**
   * Deletes rows from a table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param condition_?  the optional [[Condition]]
   * @param limit        the limit
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[QueryResult]]
   */
  def deleteRows(databaseName: String, tableName: String, condition_? : Option[Condition], limit: Option[Int])
                (implicit service: ServerSideTableService): QueryResult = {
    val (count, responseTime) = time {
      val table = service(databaseName, tableName)
      table.delete(condition = toCriteria(condition_?), limit = limit)
    }
    QueryResult(databaseName, tableName, count = count, responseTime = responseTime)
  }

  /**
   * Inserts rows into a table
   * @param databaseName  the database name
   * @param tableName     the table name
   * @param fields        the collection of fields
   * @param rowValuesList the row data
   * @param service       the implicit [[ServerSideTableService]]
   * @return the [[QueryResult]]
   */
  def insertRows(databaseName: String, tableName: String, fields: Seq[String], rowValuesList: List[List[Any]])
                (implicit service: ServerSideTableService): QueryResult = {
    val (results, responseTime) = time {
      val table = service(databaseName, tableName)
      for {
        rowValues <- rowValuesList
        rowID = table.insert(values = Map(fields zip rowValues: _*))
      } yield rowID
    }
    QueryResult(databaseName, tableName, count = results.size, __ids = results, responseTime = responseTime)
  }

  /**
   * Retrieves rows from a table
   * @param databaseName the database name
   * @param select       the [[Select]]
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[QueryResult]]
   */
  def selectRows(databaseName: String, select: Select)(implicit service: ServerSideTableService): QueryResult = {
    val startTime = System.nanoTime()
    val elapsedTime = () => (System.nanoTime() - startTime) / 1e+6
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = service(databaseName, tableName)
        val results = table.executeQuery(toCriteria(select.where), limit = select.limit) // TODO select.fields & select.orderBy
        QueryResult(databaseName, tableName, columns = table.device.columns.map(_.toTableColumn),
          rows = results.map(_.fields.map(_.value)), __ids = results.map(_.rowID),
          responseTime = elapsedTime())
      case Some(queryable) => throw new IllegalArgumentException(s"Unsupported queryable $queryable")
      case None => QueryResult(databaseName, tableName = "", responseTime = elapsedTime())
    }
  }

  /**
   * Truncates a table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[QueryResult]]
   */
  def truncateTable(databaseName: String, tableName: String)(implicit service: ServerSideTableService): QueryResult = {
    val (count, responseTime) = time(service(databaseName, tableName).truncate())
    QueryResult(databaseName, tableName, count = count, responseTime = responseTime)
  }

  private def toCriteria(condition_? : Option[Condition]): TupleSet = {
    condition_? match {
      case Some(ConditionalOp(Field(name), value, "==", "=")) => Map(name -> value.translate)
      case Some(condition) => throw new IllegalArgumentException(s"Unsupported condition $condition")
      case None => Map.empty
    }
  }

  /**
   * Implicit classes and conversions
   */
  object implicits {

    final implicit class ExpressionFacade(val expression: Expression) extends AnyVal {
      def translate: Any = expression match {
        case Literal(value) => value
        case unknown => throw new IllegalArgumentException(s"Unsupported value $unknown")
      }
    }

    final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
      def invoke(databaseName: String)(implicit service: ServerSideTableService): QueryResult = invokable match {
        case Create(table: Table) => createTable(databaseName, table)
        case Create(TableIndex(indexName, table, columns)) => createTableIndex(databaseName, indexName, table, columns)
        case Delete(ref, condition_?, limit) => deleteRows(databaseName, tableName = ref.name, condition_?, limit)
        case DropTable(TableRef(tableName)) => service.dropTable(databaseName, tableName)
        case Insert(Into(TableRef(tableName)), Insert.Values(expressionValues), fields) =>
          insertRows(databaseName, tableName, fields.map(_.name), expressionValues.map(_.map(_.translate)))
        case select: Select => selectRows(databaseName, select)
        case Truncate(TableRef(tableName)) => truncateTable(databaseName, tableName)
        case unknown => throw new IllegalArgumentException(s"Unsupported operation $unknown")
      }
    }

  }

}

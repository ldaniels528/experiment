package com.qwery.database

import com.qwery.database.DatabaseCPU.implicits._
import com.qwery.database.DatabaseCPU.{Solution, toCriteria}
import com.qwery.database.ExpressionVM.evaluate
import com.qwery.database.device.{BlockDevice, TableIndexDevice}
import com.qwery.database.files.DatabaseFiles.isTableFile
import com.qwery.database.files.{TableColumn, TableFile, TableMetrics, TableProperties, VirtualTableFile}
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Insert.Into
import com.qwery.models.expressions._
import com.qwery.models.{expressions => ex, _}
import com.qwery.{models => mx}

import scala.collection.concurrent.TrieMap

/**
  * Database CPU
  */
class DatabaseCPU() {
  private val tables = TrieMap[(String, String), TableFile]()
  private val views = TrieMap[(String, String), VirtualTableFile]()

  def countRows(databaseName: String, tableName: String, condition: Option[Condition], limit: Option[Int] = None): Long = {
    if (isTableFile(databaseName, tableName))
      tableOf(databaseName, tableName).countRows(toCriteria(condition), limit)
    else
      viewOf(databaseName, tableName).countRows(toCriteria(condition), limit)
  }

  /**
    * Creates a new column index on a database table
    * @param databaseName    the database name
    * @param tableName       the table name
    * @param indexColumnName the index column name
    * @param ifNotExists     if false, an error when the table already exists
    * @return the [[TableIndexDevice index device]]
    */
  def createIndex(databaseName: String, tableName: String, indexColumnName: String, ifNotExists: Boolean = false): TableIndexDevice = {
    tableOf(databaseName, tableName).createIndex(indexColumnName)
  }

  /**
    * Creates a new table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param properties   the [[TableProperties table properties]]
    * @return the [[TableFile]]
    */
  def createTable(databaseName: String, tableName: String, properties: TableProperties): TableFile = {
    val table = TableFile.createTable(databaseName, tableName, properties)
    tables(databaseName -> tableName) = table
    table
  }

  /**
    * Creates a new view (virtual table)
    * @param databaseName the database name
    * @param viewName     the view name
    * @param description  the optional description or remarks
    * @param invokable    the [[Invokable SQL query]]
    * @param ifNotExists  if true, the operation will not fail
    * @return true, if the view was created
    */
  def createView(databaseName: String, viewName: String, description: Option[String], invokable: Invokable, ifNotExists: Boolean): VirtualTableFile = {
    val view = VirtualTableFile.createView(databaseName, viewName, description, invokable, ifNotExists)
    views(databaseName -> viewName) = view
    view
  }

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnName: String): Unit = {
    tableOf(databaseName, tableName).deleteField(rowID, columnName)
  }

  /**
    * Deletes a row by ID
    * @param databaseName the database name
    * @param tableName    the table name
    * @param rowID        the ID of the row to delete
    */
  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): Unit = {
    tableOf(databaseName, tableName).deleteRow(rowID)
  }

  /**
    * Deletes rows matching the given criteria (up to the optionally specified limit)
    * @param databaseName the database name
    * @param tableName    the table name
    * @param condition    the deletion criteria
    * @param limit        the maximum number of records to delete
    * @return the number of rows affected
    */
  def deleteRows(databaseName: String, tableName: String, condition: Option[Condition], limit: Option[Int] = None): Long = {
    tableOf(databaseName, tableName).deleteRows(condition = toCriteria(condition), limit)
  }

  /**
    * Deletes a database table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param ifExists     indicates whether an existence check should be performed
    * @return true, if the table was dropped
    */
  def dropTable(databaseName: String, tableName: String, ifExists: Boolean = false): Boolean = {
    TableFile.dropTable(databaseName, tableName, ifExists)
    tables.remove(databaseName -> tableName).nonEmpty
  }

  /**
    * Deletes a database view
    * @param databaseName the database name
    * @param viewName     the virtual table name
    * @param ifExists     indicates whether an existence check should be performed
    * @return true, if the view was dropped
    */
  def dropView(databaseName: String, viewName: String, ifExists: Boolean = false): Boolean = {
    VirtualTableFile.dropView(databaseName, viewName, ifExists)
    views.remove(databaseName -> viewName).nonEmpty
  }

  /**
    * Executes an invokable
    * @param databaseName the database name
    * @param invokable    the [[Invokable invokable]]
    * @return the [[Solution results]]
    */
  def execute(databaseName: String, invokable: Invokable): Solution = {
    invokable match {
      case Create(TableIndex(_, TableRef(tableName), Seq(ex.Field(indexColumn)), ifNotExists)) =>
        Solution(databaseName, tableName, createIndex(databaseName, tableName, indexColumn, ifNotExists))
      case Create(Table(tableName, columns, ifNotExists, isColumnar, isPartitioned, description)) =>
        Solution(databaseName, tableName, createTable(databaseName, tableName, TableProperties(description, columns.map(_.toTableColumn), isColumnar, ifNotExists)))
      case Create(View(viewName, invokable, description, ifNotExists)) =>
        Solution(databaseName, viewName, createView(databaseName, viewName, description, invokable, ifNotExists))
      case Delete(TableRef(tableName), where, limit) =>
        Solution(databaseName, tableName, deleteRows(databaseName, tableName, where, limit))
      case DropTable(TableRef(tableName), ifExists) =>
        Solution(databaseName, tableName, dropTable(databaseName, tableName, ifExists))
      case DropView(TableRef(viewName), ifExists) =>
        Solution(databaseName, viewName, dropView(databaseName, viewName, ifExists))
      case Insert(Into(TableRef(tableName)), Insert.Values(values), fields) =>
        Solution(databaseName, tableName, insertRows(databaseName, tableName, fields = fields.map(_.name), values = values))
      case Insert(Into(TableRef(tableName)), queryable, fields) =>
        val solution = execute(databaseName, queryable)
        Solution(databaseName, tableName, insertRows(databaseName, tableName, device = solution.result.asInstanceOf[BlockDevice]))
      case Select(Seq(FunctionCall("count", List(AllFields))), Some(TableRef(tableName)), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where@None, limit@None) =>
        Solution(databaseName, tableName, getDevice(databaseName, tableName).countRows(_.isActive))
      case Select(Seq(FunctionCall("count", List(AllFields))), Some(TableRef(tableName)), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where, limit) =>
        Solution(databaseName, tableName, countRows(databaseName, tableName, where, limit))
      case Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
        Solution(databaseName, tableName, selectRows(databaseName, tableName, fields, where, groupBy, orderBy, limit))
      case Truncate(TableRef(tableName)) =>
        Solution(databaseName, tableName, truncateTable(databaseName, tableName))
      case Update(TableRef(tableName), changes, where, limit) =>
        Solution(databaseName, tableName, updateRows(databaseName, tableName, changes, where, limit))
      case unhandled => die(s"Unhandled instruction: $unhandled")
    }
  }

  /**
    * Executes a SQL statement or query
    * @param databaseName the database name
    * @param sql          the SQL statement or query
    * @return either a [[BlockDevice block device]] containing a result set or an update count
    */
  def executeQuery(databaseName: String, sql: String): Either[BlockDevice, Long] = {
    val solution = execute(databaseName, invokable = SQLLanguageParser.parse(sql))
    solution.result match {
      case device: BlockDevice => Left(device)
      case file: TableFile => Left(file.device)
      case file: VirtualTableFile => Left(file.device)
      case outcome: Boolean => Right(if(outcome) 1 else 0)
      case count: Int => Right(count)
      case count: Long => Right(count)
      case ids: Seq[_] => Right(ids.length)
      case _: Unit => Right(1)
      case xx => die(s"executeQuery: unhandled $xx")
    }
  }

  def getColumns(databaseName: String, tableName: String): Seq[Column] = getDevice(databaseName, tableName).columns

  def getDevice(databaseName: String, tableName: String): BlockDevice = {
    if (isTableFile(databaseName, tableName)) tableOf(databaseName, tableName).device
    else viewOf(databaseName, tableName).device
  }

  /**
    * Deletes a range of rows in the database
    * @param databaseName the database name
    * @param tableName    the table name
    * @param start        the initial row ID
    * @param length       the number of rows to delete
    * @return the update count
    */
  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: Int): Long = {
    tableOf(databaseName, tableName).deleteRange(start, length)
  }

  /**
    * Retrieves a range of records
    * @param databaseName the database name
    * @param tableName    the table name
    * @param start        the beginning of the range
    * @param length       the number of records to retrieve
    * @return a [[BlockDevice]] containing the rows
    */
  def getRange(databaseName: String, tableName: String, start: ROWID, length: Int): BlockDevice = {
    tableOf(databaseName, tableName).getRange(start, length)
  }

  /**
    * Retrieves a row by ID
    * @param databaseName the database name
    * @param tableName    the table name
    * @param rowID        the row ID
    * @return the option of a [[Row row]]
    */
  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[Row] = {
    val row = getDevice(databaseName, tableName).getRow(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  def getTableLength(databaseName: String, tableName: String): Long = getDevice(databaseName, tableName).length

  def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    tableOf(databaseName, tableName).getTableMetrics
  }

  /**
    * Appends a new row to the specified database table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param values       the list of [[Insert.DataRow value sets]]
    * @return the new row's ID
    */
  def insertRow(databaseName: String, tableName: String, values: Seq[(String, Expression)]): ROWID = {
    implicit val scope: Scope = Scope()
    val _values = values.map { case (name, expr) => name -> evaluate(expr).value.orNull }
    tableOf(databaseName, tableName).insertRow(KeyValues(_values: _*))
  }

  /**
    * Appends new rows to the specified database table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param device       the [[BlockDevice]] containing the rows
    * @return the number of rows inserted
    */
  def insertRows(databaseName: String, tableName: String, device: BlockDevice): Int = {
    tableOf(databaseName, tableName).insertRows(device)
  }

  /**
    * Appends new rows to the specified database table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param fields       the collection of field names
    * @param values       the list of [[Insert.DataRow value sets]]
    * @return the collection of row IDs
    */
  def insertRows(databaseName: String, tableName: String, fields: Seq[String], values: Seq[Seq[Expression]]): Seq[ROWID] = {
    implicit val scope: Scope = Scope()
    val _values = values.map(_.map { expr => evaluate(expr).value.orNull })
    tableOf(databaseName, tableName).insertRows(fields, _values)
  }

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): Unit = {
    tableOf(databaseName, tableName).replaceRow(rowID, values)
  }

  def lockRow(databaseName: String, tableName: String, rowID: ROWID): Unit = {
    tableOf(databaseName, tableName).lockRow(rowID)
  }

  /**
    * Executes a query
    * @param databaseName the database name
    * @param tableName    the table name
    * @param fields       the [[Expression field projection]]
    * @param where        the condition which determines which records are included
    * @param groupBy      the optional aggregation columns
    * @param orderBy      the columns to order by
    * @param limit        the optional limit
    * @return a [[BlockDevice]] containing the rows
    */
  def selectRows(databaseName: String, tableName: String,
                 fields: Seq[Expression],
                 where: Option[Condition],
                 groupBy: Seq[ex.Field] = Nil,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    if (isTableFile(databaseName, tableName))
      tableOf(databaseName, tableName).selectRows(fields, where = toCriteria(where), groupBy, orderBy, limit)
    else
      viewOf(databaseName, tableName).selectRows(fields, where = toCriteria(where), groupBy, orderBy, limit)
  }

  /**
    * Truncates the table; removing all rows
    * @return the number of rows removed
    */
  def truncateTable(databaseName: String, tableName: String): Long = {
    tableOf(databaseName, tableName).truncate()
  }

  def unlockRow(databaseName: String, tableName: String, rowID: ROWID, lockID: String): Unit = {
    tableOf(databaseName, tableName).unlockRow(rowID)
  }

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, changes: Seq[(String, Expression)]): Unit = {
    tableOf(databaseName, tableName).updateRow(rowID, KeyValues(changes: _*))
  }

  def updateRows(databaseName: String, tableName: String, changes: Seq[(String, Expression)], condition: Option[Condition], limit: Option[Int] = None): Long = {
    tableOf(databaseName, tableName).updateRows(KeyValues(changes: _*), toCriteria(condition), limit)
  }

  private def tableOf(databaseName: String, tableName: String): TableFile = {
    tables.getOrElseUpdate(databaseName -> tableName, TableFile(databaseName, tableName))
  }

  private def viewOf(databaseName: String, tableName: String): VirtualTableFile = {
    views.getOrElseUpdate(databaseName -> tableName, VirtualTableFile(databaseName, tableName))
  }

}

/**
  * Database CPU Companion
  */
object DatabaseCPU {
  val columnTypeMap = Map(
    "ARRAY" -> ColumnTypes.ArrayType,
    "BIGINT" -> ColumnTypes.BigIntType,
    "BINARY" -> ColumnTypes.BinaryType,
    "BLOB" -> ColumnTypes.BlobType,
    "BOOLEAN" -> ColumnTypes.BooleanType,
    "CHAR" -> ColumnTypes.StringType,
    "CLOB" -> ColumnTypes.ClobType,
    "DATE" -> ColumnTypes.DateType,
    "DATETIME" -> ColumnTypes.DateType,
    "DECIMAL" -> ColumnTypes.BigDecimalType,
    "DOUBLE" -> ColumnTypes.DoubleType,
    "FLOAT" -> ColumnTypes.FloatType,
    "INT" -> ColumnTypes.IntType,
    "INTEGER" -> ColumnTypes.IntType,
    "LONG" -> ColumnTypes.LongType,
    "OBJECT" -> ColumnTypes.SerializableType,
    "REAL" -> ColumnTypes.DoubleType,
    "SHORT" -> ColumnTypes.ShortType,
    "SMALLINT" -> ColumnTypes.ShortType,
    "STRING" -> ColumnTypes.StringType,
    "TEXT" -> ColumnTypes.ClobType,
    "TIMESTAMP" -> ColumnTypes.DateType,
    "TINYINT" -> ColumnTypes.ByteType,
    "UUID" -> ColumnTypes.UUIDType,
    "VARCHAR" -> ColumnTypes.StringType
  )

  def toCriteria(condition_? : Option[Condition]): KeyValues = condition_? match {
    case Some(ConditionalOp(ex.Field(name), Literal(value), "==", "=")) => KeyValues(name -> value)
    case Some(condition) => die(s"Unsupported condition $condition")
    case None => KeyValues()
  }

  case class Solution(databaseName: String, tableName: String, result: Any)

  /**
    * Implicit definitions
    */
  object implicits {

    /**
      * SQL Column-To-Column Conversion
      * @param column the [[mx.Column SQL Column]]
      */
    final implicit class SQLToColumnConversion(val column: mx.Column) extends AnyVal {
      @inline
      def toColumn: Column = Column(
        name = column.name,
        comment = column.comment.getOrElse(""),
        enumValues = column.enumValues,
        maxSize = column.spec.precision.headOption,
        metadata = ColumnMetadata(
          isNullable = column.isNullable,
          `type` = columnTypeMap.getOrElse(column.spec.typeName, ColumnTypes.BlobType)
        ))

      @inline
      def toTableColumn: TableColumn = {
        val columnType = columnTypeMap.getOrElse(column.spec.typeName, ColumnTypes.BlobType)
        TableColumn(
          name = column.name,
          columnType = columnType.toString,
          comment = column.comment,
          enumValues = column.enumValues,
          sizeInBytes = columnType.getFixedLength.getOrElse(column.spec.precision.headOption
            .getOrElse(die(s"Column size could not be determined for '$columnType'"))))
      }
    }

  }

}

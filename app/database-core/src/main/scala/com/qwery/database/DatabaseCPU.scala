package com.qwery.database

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.DatabaseCPU.implicits._
import com.qwery.database.DatabaseCPU.{Solution, toCriteria}
import com.qwery.database.ExpressionVM.{RichCondition, evaluate, nextID}
import com.qwery.database.device.{BlockDevice, TableIndexDevice}
import com.qwery.database.files.DatabaseFiles.{isVirtualTable, readTableConfig}
import com.qwery.database.files._
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models.expressions._
import com.qwery.models.{expressions => ex, _}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._
import com.qwery.{models => mx}
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
  * Database CPU
  */
class DatabaseCPU() {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tables = TrieMap[(String, String), TableFileLike]()

  /**
   * Counts the number of rows matching the optional criteria
   * @param databaseName the database name
   * @param tableName    the table name
   * @param condition    the optional [[Condition criteria]]
   * @param limit        the optional limit
   * @return the number of rows matching the optional criteria
   */
  def countRows(databaseName: String, tableName: String, condition: Option[Condition], limit: Option[Int] = None): Long = {
    tableOf(databaseName, tableName).countRows(toCriteria(condition), limit)
  }

  /**
    * Creates a reference to an external table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param ref          the [[ExternalTable table properties]]
    */
  def createExternalTable(databaseName: String, tableName: String, ref: ExternalTable): Unit = {
    tables(databaseName -> tableName) = ExternalTableFile.createTable(databaseName, ref)
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
    */
  def createTable(databaseName: String, tableName: String, properties: TableProperties): Unit = {
    tables(databaseName -> tableName) = TableFile.createTable(databaseName, tableName, properties)
  }

  /**
    * Creates a new view (virtual table)
    * @param databaseName the database name
    * @param viewName     the view name
    * @param description  the optional description or remarks
    * @param invokable    the [[Invokable SQL query]]
    * @param ifNotExists  if true, the operation will not fail
    */
  def createView(databaseName: String, viewName: String, description: Option[String], invokable: Invokable, ifNotExists: Boolean): Unit = {
    tables(databaseName -> viewName) = VirtualTableFile.createView(databaseName, viewName, description, invokable, ifNotExists)
  }

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Unit = {
    tableOf(databaseName, tableName).deleteField(rowID, columnID)
  }

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnName: String): Unit = {
    tableOf(databaseName, tableName).deleteField(rowID, columnName)
  }

  /**
    * Retrieves a field by row and column IDs
    * @param databaseName the database name
    * @param tableName    the table name
    * @param rowID        the row ID
    * @param columnID     the column ID
    * @return the [[Field field]]
    */
  def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Field = {
    tableOf(databaseName, tableName).getField(rowID, columnID)
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, newValue: Option[Any]): Unit = {
    tableOf(databaseName, tableName).updateField(rowID, columnID, newValue)
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnName: String, newValue: Option[Any]): Unit = {
    tableOf(databaseName, tableName).updateField(rowID, columnName, newValue)
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
    tables.remove(databaseName -> viewName).nonEmpty
  }

  /**
    * Executes an invokable
    * @param databaseName the database name
    * @param invokable    the [[Invokable invokable]]
    * @return the [[Solution solution]] containing a result set or an update count
    */
  def execute(databaseName: String, invokable: Invokable): Option[Solution] = {
    invokable match {
      case Console.Debug(message) => logger.debug(message); None
      case Console.Error(message) => logger.error(message); None
      case Console.Info(message) => logger.info(message); None
      case Console.Print(message) => println(message); None
      case Console.Warn(message) => logger.warn(message); None
      case Create(ref: ExternalTable) =>
        createExternalTable(databaseName, tableName = ref.name, ref = ref); None
      case Create(Table(tableName, columns, ifNotExists, isColumnar, isPartitioned, description)) =>
        Some(Solution(databaseName, tableName, createTable(databaseName, tableName, TableProperties(description, columns.map(_.toColumn), isColumnar, ifNotExists))))
      case Create(TableIndex(_, TableRef(tableName), Seq(ex.Field(indexColumn)), ifNotExists)) =>
        Some(Solution(databaseName, tableName, createIndex(databaseName, tableName, indexColumn, ifNotExists)))
      case Create(View(viewName, invokable, description, ifNotExists)) =>
        Some(Solution(databaseName, viewName, createView(databaseName, viewName, description, invokable, ifNotExists)))
      case Delete(TableRef(tableName), where, limit) =>
        Some(Solution(databaseName, tableName, deleteRows(databaseName, tableName, where, limit)))
      case DropTable(TableRef(tableName), ifExists) =>
        Some(Solution(databaseName, tableName, dropTable(databaseName, tableName, ifExists)))
      case DropView(TableRef(viewName), ifExists) =>
        Some(Solution(databaseName, viewName, dropView(databaseName, viewName, ifExists)))
      case ForLoop(variable, rows, invokable, isReverse) => forLoop(variable, rows, invokable, isReverse)
      case Include(path) => execute(databaseName, invokable = include(path))
      case Insert(Into(TableRef(tableName)), Insert.Values(values), fields) =>
        Some(Solution(databaseName, tableName, insertRows(databaseName, tableName, fields = fields.map(_.name), values = values)))
      case Insert(Into(TableRef(tableName)), queryable, fields) =>
        Some(Solution(databaseName, tableName, insertRows(databaseName, tableName, toDevice(databaseName, queryable), overwrite = false)))
      case Insert(Overwrite(TableRef(tableName)), queryable, fields) =>
        Some(Solution(databaseName, tableName, insertRows(databaseName, tableName, toDevice(databaseName, queryable), overwrite = true)))
      case Select(Seq(fc@FunctionCall("count", List(AllFields))), Some(TableRef(tableName)), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where@None, limit@None) =>
        Some(Solution(databaseName, tableName, countRowsAsDevice(fc.alias || nextID, () => getDevice(databaseName, tableName).countRows(_.isActive))))
      case Select(Seq(fc@FunctionCall("count", List(AllFields))), Some(TableRef(tableName)), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where, limit) =>
        Some(Solution(databaseName, tableName, countRowsAsDevice(fc.alias || nextID, () => countRows(databaseName, tableName, where, limit))))
      case Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
        Some(Solution(databaseName, tableName, selectRows(databaseName, tableName, fields, where, groupBy, having, orderBy, limit)))
      case Show(invokable, limit) => show(databaseName, invokable, limit)
      case SQL(ops) => ops.foldLeft[Option[Solution]](None) { (_, op) => execute(databaseName, op) }
      case Truncate(TableRef(tableName)) =>
        Some(Solution(databaseName, tableName, truncateTable(databaseName, tableName)))
      case Update(TableRef(tableName), changes, where, limit) =>
        Some(Solution(databaseName, tableName, updateRows(databaseName, tableName, changes, where, limit)))
      case While(condition, invokable) => `while`(databaseName, condition, invokable)
      case unhandled => die(s"Unhandled instruction: $unhandled")
    }
  }

  /**
    * Executes a SQL statement or query
    * @param databaseName the database name
    * @param sql          the SQL statement or query
    * @return the [[Solution solution]] containing a result set or an update count
    */
  def executeQuery(databaseName: String, sql: String): Option[Solution] = {
    execute(databaseName, invokable = SQLLanguageParser.parse(sql))
  }

  /**
    * Executes a SQL script
    * @param databaseName the database name
    * @param file         the the SQL script [[File file]]
    * @return the [[Solution solution]] containing a result set or an update count
    */
  def executeScript(databaseName: String, file: File): Option[Solution] = {
    executeQuery(databaseName, sql = Source.fromFile(file).use(_.mkString))
  }

  /**
    * FOR-LOOP statement
    * @param variable  the given [[RowSetVariableRef variable]]
    * @param rows      the given [[Invokable rows]]
    * @param invokable the [[Invokable statements]] to execute
    * @param isReverse indicates reverse order
    */
  def forLoop(variable: RowSetVariableRef,
              rows: Invokable,
              invokable: Invokable,
              isReverse: Boolean): Option[Solution] = ???

  /**
    * Returns the columns
    * @param databaseName the database name
    * @param tableName    the table name
    * @return the [[Column columns]]
    */
  def getColumns(databaseName: String, tableName: String): Seq[Column] = getDevice(databaseName, tableName).columns

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

  /**
    * Retrieves rows matching the given condition up to the optional limit
    * @param databaseName the database name
    * @param tableName    the table name
    * @param condition    the given [[KeyValues condition]]
    * @param limit        the optional limit
    * @return the [[BlockDevice results]]
    */
  def getRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int] = None): BlockDevice = {
    tableOf(databaseName, tableName).getRows(condition, limit)
  }

  /**
   * Returns the length of the given table
   * @param databaseName the database name
   * @param tableName    the table name
   * @return the length of the given table
   */
  def getTableLength(databaseName: String, tableName: String): Long = getDevice(databaseName, tableName).length

  /**
   * Returns the metrics for the given table
   * @param databaseName the database name
   * @param tableName    the table name
   * @return the [[TableMetrics table metrics]]
   */
  def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    tableOf(databaseName, tableName).getTableMetrics
  }

  /**
    * Incorporates the source code of the given path
    * @param path the given .sql source file
    * @return the resultant source code
    */
  def include(path: String): Invokable = {
    val file = new File(path).getCanonicalFile
    logger.info(s"Merging source file '${file.getAbsolutePath}'...")
    SQLLanguageParser.parse(file)
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
  def insertRows(databaseName: String, tableName: String, device: BlockDevice, overwrite: Boolean): Int = {
    if (overwrite) getDevice(databaseName, tableName).shrinkTo(newSize = 0)
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
    * @param having       the aggregate condition which determines which records are included
    * @param orderBy      the columns to order by
    * @param limit        the optional limit
    * @return a [[BlockDevice block device]] containing the rows
    */
  def selectRows(databaseName: String, tableName: String,
                 fields: Seq[Expression],
                 where: Option[Condition],
                 groupBy: Seq[ex.Field] = Nil,
                 having: Option[Condition] = None,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    tableOf(databaseName, tableName).selectRows(fields, where = toCriteria(where), groupBy, having, orderBy, limit)
  }

  def show(databaseName: String, invokable: Invokable, limit: Option[Int] = None): Option[Solution] = {
    execute(databaseName, invokable).map(_.get) match {
      case Some(Left(device)) =>
        (0 until limit.getOrElse(20)).map(device.getRow(_)).foreach { row =>
          logger.info(f"[${row.id}02d] ${row.toKeyValues}")
        }
      case Some(Right(count)) => logger.info(s"w = $count")
      case _ =>
    }
    None
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
    implicit val scope: Scope = Scope()
    val row = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
    tableOf(databaseName, tableName).updateRow(rowID, row)
  }

  def updateRows(databaseName: String, tableName: String, changes: Seq[(String, Expression)], condition: Option[Condition], limit: Option[Int] = None): Long = {
    implicit val scope: Scope = Scope()
    val row = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
    tableOf(databaseName, tableName).updateRows(row, toCriteria(condition), limit)
  }

  def `while`(databaseName: String, condition: Condition, invokable: Invokable): Option[Solution] = {
    var result: Option[Solution] = None
    implicit val scope: Scope = Scope()
    while (condition.isTrue) {
      result = execute(databaseName, invokable)
    }
    result
  }

  private def countRowsAsDevice(name: String, counter: () => Long): BlockDevice = {
    val rows = createTempTable(columns = Seq(Column.create(name, metadata = ColumnMetadata(`type` = ColumnTypes.LongType))), fixedRowCount = 1)
    rows.writeRow(KeyValues(name -> counter()).toBinaryRow(rows))
    rows
  }

  /**
    * Returns the block device
    * @param databaseName the database name
    * @param tableName    the table name
    * @return the [[BlockDevice block device]]
    */
  private def getDevice(databaseName: String, tableName: String): BlockDevice = {
    tableOf(databaseName, tableName).device
  }

  private def tableOf(databaseName: String, tableName: String): TableFileLike = {
    tables.getOrElseUpdate(databaseName -> tableName, {
      val config = readTableConfig(databaseName, tableName)
      if (config.externalTable.nonEmpty) ExternalTableFile(databaseName, tableName)
      else if (isVirtualTable(databaseName, tableName)) VirtualTableFile(databaseName, tableName)
      else TableFile(databaseName, tableName)
    })
  }

  private def toDevice(databaseName: String, queryable: Queryable): BlockDevice = {
    execute(databaseName, queryable).map(_.get) match {
      case Some(Left(results: BlockDevice)) => results
      case unknown => die(s"Query result expected, but got '$unknown' instead")
    }
  }

}

/**
  * Database CPU Companion
  */
object DatabaseCPU {
  private val columnTypeMappings = Map(
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

  def lookupColumnType(typeName: String): ColumnType = {
    columnTypeMappings.getOrElse(typeName.toUpperCase, ColumnTypes.BlobType)
  }

  def toCriteria(condition_? : Option[Condition]): KeyValues = condition_? match {
    case Some(ConditionalOp(ex.Field(name), Literal(value), "==", "=")) => KeyValues(name -> value)
    case Some(condition) => die(s"Unsupported condition $condition")
    case None => KeyValues()
  }

  case class Solution(databaseName: String, tableName: String, result: Any) {

    /**
      * @return a normalized copy of then result as either a [[BlockDevice block device]] or [[Long update count]].
      */
    def get: Either[BlockDevice, ROWID] = {
      result match {
        case device: BlockDevice => Left(device)
        case file: TableFile => Left(file.device)
        case file: VirtualTableFile => Left(file.device)
        case outcome: Boolean => Right(if (outcome) 1 else 0)
        case count: Int => Right(count)
        case count: Long => Right(count)
        case ids: Seq[_] => Right(ids.length)
        case _: Unit => Right(1)
        case xx => die(s"executeQuery: unhandled $xx")
      }
    }
  }

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
      def toColumn: Column = Column.create(
        name = column.name,
        comment = column.comment.getOrElse(""),
        enumValues = column.enumValues,
        maxSize = column.spec.precision.headOption,
        metadata = ColumnMetadata(
          isNullable = column.isNullable,
          `type` = lookupColumnType(column.spec.typeName)
        ))
    }

  }

}

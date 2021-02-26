package com.qwery.database

import com.qwery.database.DatabaseCPU.implicits.InvokableWithDatabase
import com.qwery.database.DatabaseCPU.{Solution, toCriteria}
import com.qwery.database.ExpressionVM.{RichCondition, evaluate, nextID}
import com.qwery.database.device.{BlockDevice, TableIndexDevice}
import com.qwery.database.files.DatabaseFiles.{isVirtualTable, readTableConfig}
import com.qwery.database.files._
import com.qwery.database.models.ColumnTypes.ColumnType
import com.qwery.database.models.{Column, ColumnMetadata, ColumnTypes, KeyValues, Row, TableMetrics}
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
  private val tables = TrieMap[TableRef, TableFileLike]()

  /**
    * Counts the number of rows matching the optional criteria
    * @param ref       the [[TableRef table reference]]
    * @param condition the optional [[Condition criteria]]
    * @param limit     the optional limit
    * @return the number of rows matching the optional criteria
    */
  def countRows(ref: TableRef, condition: Option[Condition], limit: Option[Int] = None): Long = {
    tableOf(ref).countRows(toCriteria(condition), limit)
  }

  /**
    * Creates a reference to an external table
    * @param databaseName the database name
    * @param table        the [[ExternalTable table properties]]
    */
  def createExternalTable(databaseName: String, table: ExternalTable): Unit = {
    tables(table.ref) = ExternalTableFile.createTable(databaseName, table)
  }

  /**
    * Creates a new column index on a database table
    * @param ref             the [[TableRef]]
    * @param indexColumnName the index column name
    * @param ifNotExists     if false, an error when the table already exists
    * @return the [[TableIndexDevice index device]]
    */
  def createIndex(ref: TableRef, indexColumnName: String, ifNotExists: Boolean = false): TableIndexDevice = {
    tableOf(ref).createIndex(ref, indexColumnName)
  }

  /**
    * Creates a new table
    * @param databaseName the database name
    * @param table        the [[Table table properties]]
    */
  def createTable(databaseName: String, table: Table): Unit = {
    tables(table.ref) = TableFile.createTable(databaseName, table)
  }

  /**
    * Creates a new view (virtual table)
    * @param ref         the [[TableRef table reference]]
    * @param description the optional description or remarks
    * @param invokable   the [[Invokable SQL query]]
    * @param ifNotExists if true, the operation will not fail
    */
  def createView(ref: TableRef, description: Option[String], invokable: Invokable, ifNotExists: Boolean): Unit = {
    tables(ref) = VirtualTableFile.createView(ref, description, invokable, ifNotExists)
  }

  def deleteField(ref: TableRef, rowID: ROWID, columnID: Int): Unit = {
    tableOf(ref).deleteField(rowID, columnID)
  }

  def deleteField(ref: TableRef, rowID: ROWID, columnName: String): Unit = {
    tableOf(ref).deleteField(rowID, columnName)
  }

  /**
    * Retrieves a field by row and column IDs
    * @param ref      the [[TableRef table reference]]
    * @param rowID    the row ID
    * @param columnID the column ID
    * @return the [[Field field]]
    */
  def getField(ref: TableRef, rowID: ROWID, columnID: Int): models.Field = {
    tableOf(ref).getField(rowID, columnID)
  }

  def updateField(ref: TableRef, rowID: ROWID, columnID: Int, newValue: Option[Any]): Unit = {
    tableOf(ref).updateField(rowID, columnID, newValue)
  }

  def updateField(ref: TableRef, rowID: ROWID, columnName: String, newValue: Option[Any]): Unit = {
    tableOf(ref).updateField(rowID, columnName, newValue)
  }

  /**
    * Deletes a row by ID
    * @param ref   the [[TableRef table reference]]
    * @param rowID the ID of the row to delete
    */
  def deleteRow(ref: TableRef, rowID: ROWID): Unit = {
    tableOf(ref).deleteRow(rowID)
  }

  /**
    * Deletes rows matching the given criteria (up to the optionally specified limit)
    * @param ref       the [[TableRef table reference]]
    * @param condition the deletion criteria
    * @param limit     the maximum number of records to delete
    * @return the number of rows affected
    */
  def deleteRows(ref: TableRef, condition: Option[Condition], limit: Option[Int] = None): Long = {
    tableOf(ref).deleteRows(condition = toCriteria(condition), limit)
  }

  /**
    * Deletes a database table
    * @param ref      the [[TableRef table reference]]
    * @param ifExists indicates whether an existence check should be performed
    * @return true, if the table was dropped
    */
  def dropTable(ref: TableRef, ifExists: Boolean = false): Boolean = {
    TableFile.dropTable(ref, ifExists)
    tables.remove(ref).nonEmpty
  }

  /**
    * Deletes a database view
    * @param ref      the [[TableRef table reference]]
    * @param ifExists indicates whether an existence check should be performed
    * @return true, if the view was dropped
    */
  def dropView(ref: TableRef, ifExists: Boolean = false): Boolean = {
    VirtualTableFile.dropView(ref, ifExists)
    tables.remove(ref).nonEmpty
  }

  /**
    * Executes an invokable
    * @param databaseName the database name
    * @param invokable    the [[Invokable invokable]]
    * @return the [[Solution solution]] containing a result set or an update count
    */
  def execute(databaseName: String, invokable: Invokable): Option[Solution] = {
    invokable.withDatabase(databaseName) match {
      case Console.Debug(message) => logger.debug(message); None
      case Console.Error(message) => logger.error(message); None
      case Console.Info(message) => logger.info(message); None
      case Console.Print(message) => println(message); None
      case Console.Warn(message) => logger.warn(message); None
      case Create(table: ExternalTable) => createExternalTable(databaseName, table); None
      case Create(table: Table) => Some(Solution(table.ref, createTable(databaseName, table)))
      case Create(TableIndex(ref, table: TableRef, Seq(ex.Field(indexColumn)), ifNotExists)) =>
        Some(Solution(ref, createIndex(table, indexColumn, ifNotExists)))
      case Create(View(ref, invokable, description, ifNotExists)) =>
        Some(Solution(ref, createView(ref, description, invokable, ifNotExists)))
      case Delete(ref, where, limit) => Some(Solution(ref, deleteRows(ref, where, limit)))
      case DropTable(ref, ifExists) => Some(Solution(ref, dropTable(ref, ifExists)))
      case DropView(ref, ifExists) => Some(Solution(ref, dropView(ref, ifExists)))
      case ForLoop(variable, rows, invokable, isReverse) => forLoop(variable, rows, invokable, isReverse)
      case Include(path) => execute(databaseName, invokable = include(path))
      case Insert(Into(ref: TableRef), Insert.Values(values), fields) =>
        Some(Solution(ref, insertRows(ref, fields = fields.map(_.name), values = values)))
      case Insert(Into(ref: TableRef), queryable, fields) =>
        Some(Solution(ref, insertRows(ref, toDevice(databaseName, queryable), overwrite = false)))
      case Insert(Overwrite(ref: TableRef), queryable, fields) =>
        Some(Solution(ref, insertRows(ref, toDevice(ref.databaseName || databaseName, queryable), overwrite = true)))
      case Select(Seq(fc@FunctionCall("count", List(AllFields))), Some(ref: TableRef), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where@None, limit@None) =>
        Some(Solution(ref, countRowsAsDevice(fc.alias || nextID, () => getDevice(ref).countRows(_.isActive))))
      case Select(Seq(fc@FunctionCall("count", List(AllFields))), Some(ref: TableRef), joins@Nil, groupBy@Nil, having@None, orderBy@Nil, where, limit) =>
        Some(Solution(ref, countRowsAsDevice(fc.alias || nextID, () => countRows(ref, where, limit))))
      case Select(fields, Some(ref: TableRef), joins, groupBy, having, orderBy, where, limit) =>
        Some(Solution(ref, selectRows(ref, fields, where, groupBy, having, orderBy, limit)))
      case Show(invokable, limit) => show(databaseName, invokable, limit)
      case SQL(ops) => ops.foldLeft[Option[Solution]](None) { (_, op) => execute(databaseName, op) }
      case Truncate(ref: TableRef) => Some(Solution(ref, truncateTable(ref)))
      case Update(ref: TableRef, changes, where, limit) => Some(Solution(ref, updateRows(ref, changes, where, limit)))
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
              isReverse: Boolean): Option[Solution] = {
    die("forLoop is not yet implemented")
  }

  /**
    * Returns the columns
    * @param ref the [[TableRef table reference]]
    * @return the [[Column columns]]
    */
  def getColumns(ref: TableRef): Seq[Column] = getDevice(ref).columns

  /**
    * Deletes a range of rows in the database
    * @param ref    the [[TableRef table reference]]
    * @param start  the initial row ID
    * @param length the number of rows to delete
    * @return the update count
    */
  def deleteRange(ref: TableRef, start: ROWID, length: Int): Long = {
    tableOf(ref).deleteRange(start, length)
  }

  /**
    * Retrieves a range of records
    * @param ref    the [[TableRef table reference]]
    * @param start  the beginning of the range
    * @param length the number of records to retrieve
    * @return a [[BlockDevice]] containing the rows
    */
  def getRange(ref: TableRef, start: ROWID, length: Int): BlockDevice = {
    tableOf(ref).getRange(start, length)
  }

  /**
    * Retrieves a row by ID
    * @param ref   the [[TableRef table reference]]
    * @param rowID the row ID
    * @return the option of a [[Row row]]
    */
  def getRow(ref: TableRef, rowID: ROWID): Option[Row] = {
    val row = getDevice(ref).getRow(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  /**
    * Retrieves rows matching the given condition up to the optional limit
    * @param ref       the [[TableRef table reference]]
    * @param condition the given [[KeyValues condition]]
    * @param limit     the optional limit
    * @return the [[BlockDevice results]]
    */
  def getRows(ref: TableRef, condition: KeyValues, limit: Option[Int] = None): BlockDevice = {
    tableOf(ref).getRows(condition, limit)
  }

  /**
    * Returns the length of the given table
    * @param ref the [[TableRef table reference]]
    * @return the length of the given table
    */
  def getTableLength(ref: TableRef): Long = getDevice(ref).length

  /**
    * Returns the metrics for the given table
    * @param ref the [[TableRef table reference]]
    * @return the [[TableMetrics table metrics]]
    */
  def getTableMetrics(ref: TableRef): TableMetrics = {
    tableOf(ref).getTableMetrics
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
    * @param ref    the [[TableRef table reference]]
    * @param values the list of [[Insert.DataRow value sets]]
    * @return the new row's ID
    */
  def insertRow(ref: TableRef, values: Seq[(String, Expression)]): ROWID = {
    implicit val scope: Scope = Scope()
    val _values = values.map { case (name, expr) => name -> evaluate(expr).value.orNull }
    tableOf(ref).insertRow(KeyValues(_values: _*))
  }

  /**
    * Appends new rows to the specified database table
    * @param ref    the [[TableRef table reference]]
    * @param device the [[BlockDevice]] containing the rows
    * @return the number of rows inserted
    */
  def insertRows(ref: TableRef, device: BlockDevice, overwrite: Boolean): Int = {
    if (overwrite) getDevice(ref).shrinkTo(newSize = 0)
    tableOf(ref).insertRows(device)
  }

  /**
    * Appends new rows to the specified database table
    * @param ref    the [[TableRef table reference]]
    * @param fields the collection of field names
    * @param values the list of [[Insert.DataRow value sets]]
    * @return the collection of row IDs
    */
  def insertRows(ref: TableRef, fields: Seq[String], values: Seq[Seq[Expression]]): Seq[ROWID] = {
    implicit val scope: Scope = Scope()
    val _values = values.map(_.map { expr => evaluate(expr).value.orNull })
    tableOf(ref).insertRows(fields, _values)
  }

  def replaceRow(ref: TableRef, rowID: ROWID, values: KeyValues): Unit = {
    tableOf(ref).replaceRow(rowID, values)
  }

  def lockRow(ref: TableRef, rowID: ROWID): Unit = {
    tableOf(ref).lockRow(rowID)
  }

  /**
    * Executes a query
    * @param ref     the [[TableRef table reference]]
    * @param fields  the [[Expression field projection]]
    * @param where   the condition which determines which records are included
    * @param groupBy the optional aggregation columns
    * @param having  the aggregate condition which determines which records are included
    * @param orderBy the columns to order by
    * @param limit   the optional limit
    * @return a [[BlockDevice block device]] containing the rows
    */
  def selectRows(ref: TableRef,
                 fields: Seq[Expression],
                 where: Option[Condition],
                 groupBy: Seq[ex.Field] = Nil,
                 having: Option[Condition] = None,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    tableOf(ref).selectRows(fields, where = toCriteria(where), groupBy, having, orderBy, limit)
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
  def truncateTable(ref: TableRef): Long = tableOf(ref).truncate()

  def unlockRow(ref: TableRef, rowID: ROWID, lockID: String): Unit = tableOf(ref).unlockRow(rowID)

  def updateRow(ref: TableRef, rowID: ROWID, changes: Seq[(String, Expression)]): Unit = {
    implicit val scope: Scope = Scope()
    val row = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
    tableOf(ref).updateRow(rowID, row)
  }

  def updateRows(ref: TableRef, changes: Seq[(String, Expression)], condition: Option[Condition], limit: Option[Int] = None): Long = {
    implicit val scope: Scope = Scope()
    val row = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
    tableOf(ref).updateRows(row, toCriteria(condition), limit)
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
    * @param ref the [[TableRef table reference]]
    * @return the [[BlockDevice block device]]
    */
  private def getDevice(ref: TableRef): BlockDevice = tableOf(ref).device

  private def tableOf(ref: TableRef): TableFileLike = {
    tables.getOrElseUpdate(ref, {
      val config = readTableConfig(ref)
      if (config.externalTable.nonEmpty) ExternalTableFile(ref)
      else if (isVirtualTable(ref)) VirtualTableFile.load(ref)
      else TableFile(ref)
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

  case class Solution(ref: TableRef, result: Any) {

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

    final implicit class InvokableWithDatabase(val invokable: Invokable) extends AnyVal {
      @inline
      def withDatabase(databaseName: String): Invokable = invokable match {
        case c@Create(table: ExternalTable) => c.copy(entity = table.copy(ref = table.ref.withDatabase(databaseName)))
        case c@Create(table: Table) => c.copy(entity = table.copy(ref = table.ref.withDatabase(databaseName)))
        case c@Create(view: View) => c.copy(entity = view.copy(ref = view.ref.withDatabase(databaseName), query = view.query.withDatabase(databaseName)))
        case d: DropTable => d.copy(table = d.table.withDatabase(databaseName))
        case d: DropView => d.copy(table = d.table.withDatabase(databaseName))
        case i: Insert =>
          i.copy(
            source = i.source.withDatabase(databaseName).asInstanceOf[Queryable],
            destination = i.destination match {
              case d@Into(target: TableRef) => d.copy(target = target.withDatabase(databaseName))
              case d@Overwrite(target: TableRef) => d.copy(target = target.withDatabase(databaseName))
              case other => other
            })
        case s: Select =>
          s.copy(from = s.from.map {
            case t: TableRef => t.withDatabase(databaseName)
            case other => other
          })
        case t: Truncate => t.copy(table = t.table.withDatabase(databaseName))
        case u: Union => u.copy(query0 = u.query0.withDatabase(databaseName), query1 = u.query1.withDatabase(databaseName))
        case u: Update => u.copy(table = u.table.withDatabase(databaseName))
        case other => other
      }
    }

  }

}

package com.qwery.database

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Scheduler}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.qwery.database.ExpressionVM.evaluate
import com.qwery.database.QueryProcessor.CommandRoutingCPU
import com.qwery.database.QueryProcessor.commands._
import com.qwery.database.models._
import com.qwery.models.expressions.{Expression, Field => SQLField}
import com.qwery.models.{Insert, OrderColumn}
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
 * Query Processor
 * @param routingActors  the number of command routing actors
 * @param requestTimeout the [[FiniteDuration request timeout]]
 */
class QueryProcessor(routingActors: Int = Runtime.getRuntime.availableProcessors(), requestTimeout: FiniteDuration = 30.seconds) {
  private val actorSystem: ActorSystem = ActorSystem(name = "QueryProcessor")
  private val actorPool: ActorRef = actorSystem.actorOf(Props(new CommandRoutingCPU(requestTimeout))
    .withRouter(RoundRobinPool(nrOfInstances = routingActors)))

  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  private implicit val scheduler: Scheduler = actorSystem.scheduler
  private implicit val timeout: Timeout = requestTimeout

  /**
   * Creates a new column index on a database table
   * @param databaseName    the database name
   * @param tableName       the table name
   * @param indexColumnName the index column name
   * @return the promise of an [[UpdateCount update count]]
   */
  def createIndex(databaseName: String, tableName: String, indexColumnName: String): Future[UpdateCount] = {
    asUpdateCount { CreateIndex(databaseName, tableName, indexColumnName) }
  }

  /**
   * Creates a new table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param columns      the table columns
   * @return the promise of an [[UpdateCount update count]]
   */
  def createTable(databaseName: String, tableName: String, columns: Seq[TableColumn]): Future[UpdateCount] = {
    asUpdateCount { CreateTable(databaseName, tableName, columns) }
  }

  /**
    * Creates a new view (virtual table)
    * @param databaseName the database name
    * @param viewName     the view name
    * @param queryString  the SQL query
    * @param ifNotExists  if true, the operation will not fail
    * @return the promise of an [[UpdateCount update count]]
    */
  def createView(databaseName: String, viewName: String, queryString: String, ifNotExists: Boolean = false): Future[UpdateCount] = {
    asUpdateCount { CreateView(databaseName, viewName, queryString, ifNotExists) }
  }

  /**
   * Deletes the contents of a field; rending it null.
   * @param databaseName the database name
   * @param tableName    the table name
   * @param rowID        the row ID of the field
   * @param columnID     the column ID of the field
   * @return the promise of an [[UpdateCount update count]]
   */
  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Future[UpdateCount] = {
    asUpdateCount { DeleteField(databaseName, tableName, rowID, columnID) }
  }

  /**
   * Deletes a range of rows in the database
   * @param databaseName the database name
   * @param tableName    the table name
   * @param start        the initial row ID
   * @param length       the number of rows to delete
   * @return the promise of an [[UpdateCount update count]]
   */
  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: Int): Future[UpdateCount] = {
    asUpdateCount { DeleteRange(databaseName, tableName, start, length) }
  }

  /**
   * Deletes a row by ID
   * @param databaseName the database name
   * @param tableName    the table name
   * @param rowID        the ID of the row to delete
   * @return the promise of an [[UpdateCount update count]]
   */
  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): Future[UpdateCount] = {
    asUpdateCount { DeleteRow(databaseName, tableName, rowID) }
  }

  /**
   * Deletes rows matching the given criteria (up to the optionally specified limit)
   * @param databaseName the database name
   * @param tableName    the table name
   * @param condition    the deletion criteria
   * @param limit        the maximum number of records to delete
   * @return the promise of an [[UpdateCount update count]]
   */
  def deleteRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int]): Future[UpdateCount] = {
    asUpdateCount { DeleteRows(databaseName, tableName, condition, limit) }
  }

  /**
   * Drops a database table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param ifExists     indicates whether an existence check should be performed
   * @return the promise of an [[UpdateCount update count]]
   */
  def dropTable(databaseName: String, tableName: String, ifExists: Boolean): Future[UpdateCount] = {
    asUpdateCount { DropTable(databaseName, tableName, ifExists) }
  }

  /**
    * Drops a database view
    * @param databaseName the database name
    * @param viewName     the virtual table name
    * @param ifExists     indicates whether an existence check should be performed
    * @return the promise of an [[UpdateCount update count]]
    */
  def dropView(databaseName: String, viewName: String, ifExists: Boolean): Future[UpdateCount] = {
    asUpdateCount { DropView(databaseName, viewName, ifExists) }
  }

  /**
   * Executes a SQL statement or query
   * @param databaseName the database name
   * @param sql the SQL statement or query
   * @return the promise of a [[QueryResult]]
   */
  def executeQuery(databaseName: String, sql: String): Future[QueryResult] = {
    val command = SQLCompiler.compile(databaseName, sql)
    (this ? command) map(_.toQueryResult(command))
  }

  /**
   * Atomically retrieves and replaces a row by ID
   * @param databaseName the database name
   * @param tableName    the table name
   * @param rowID        the row ID
   * @param f            the update function to execute
   * @return the promise of the updated [[Row row]]
   */
  def fetchAndReplace(databaseName: String, tableName: String, rowID: ROWID)(f: KeyValues => KeyValues): Future[Row] = {
    asRows { FetchAndReplace(databaseName, tableName, rowID, f) } map(_.head)
  }

  /**
   * Retrieves rows matching the given criteria (up to the optionally specified limit)
   * @param databaseName the database name
   * @param tableName    the table name
   * @param condition    the deletion criteria
   * @param limit        the maximum number of records to delete
   * @return the promise of the updated [[Row row]]
   */
  def findRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int] = None): Future[Seq[Row]] = {
    asRows { FindRows(databaseName, tableName, condition, limit) }
  }

  /**
   * Retrieves the metrics for the specified database
   * @param databaseName the specified database
   * @return the promise of [[DatabaseMetrics]]
   */
  def getDatabaseMetrics(databaseName: String): Future[DatabaseMetrics] = {
    val command = GetDatabaseMetrics(databaseName)
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case DatabaseMetricsRetrieved(metrics) => metrics
      case response => throw UnhandledCommandException(command, response)
    }
  }

  def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Future[Field] = {
    val command = GetField(databaseName, tableName, rowID, columnID)
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case FieldRetrieved(field) => field
      case response => throw UnhandledCommandException(command, response)
    }
  }

  def getRange(databaseName: String, tableName: String, start: ROWID, length: Int): Future[Seq[Row]] = {
    asRows { GetRange(databaseName, tableName, start, length) }
  }

  /**
   * Retrieves a row by ID
   * @param databaseName the database name
   * @param tableName    the table name
   * @param rowID        the row ID
   * @return the promise of the option of a [[Row]]
   */
  def getRow(databaseName: String, tableName: String, rowID: ROWID): Future[Option[Row]] = {
    val command = GetRow(databaseName, tableName, rowID)
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case RowsRetrieved(rows) => rows.headOption
      case response => throw UnhandledCommandException(command, response)
    }
  }

  def getTableLength(databaseName: String, tableName: String): Future[UpdateCount] = {
    asUpdateCount { GetTableLength(databaseName, tableName) }
  }

  /**
   * Retrieves the metrics for the specified table
   * @param databaseName the database name
   * @param tableName    the table name
   * @return the promise of [[TableMetrics]]
   */
  def getTableMetrics(databaseName: String, tableName: String): Future[TableMetrics] = {
    val command = GetTableMetrics(databaseName, tableName)
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case TableMetricsRetrieved(metrics) => metrics
      case response => throw UnhandledCommandException(command, response)
    }
  }

  /**
   * Appends a new row to the specified database table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param values       the update [[KeyValues values]]
   * @return the promise of an [[UpdateCount update count]]
   */
  def insertRow(databaseName: String, tableName: String, values: KeyValues): Future[UpdateCount] = {
    asUpdateCount { InsertRow(databaseName, tableName, values) }
  }

  /**
   * Appends a collection of new rows to the specified database table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param columns      the table column names
   * @param values       the collection of update [[KeyValues values]]
   * @return the promise of an [[UpdateCount update count]]
   */
  def insertRows(databaseName: String, tableName: String, columns: Seq[String], values: List[Insert.DataRow]): Future[UpdateCount] = {
    asUpdateCount { InsertRows(databaseName, tableName, columns, values) }
  }

  def lockRow(databaseName: String, tableName: String, rowID: ROWID): Future[String] = {
    asLockUpdated { LockRow(databaseName, tableName, rowID) } map(_.lockID)
  }

  def replaceRange(databaseName: String, tableName: String, start: ROWID, length: Int, row: => KeyValues): Future[UpdateCount] = {
    asUpdateCount { ReplaceRange(databaseName, tableName, start, length, row) }
  }

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): Future[UpdateCount] = {
    asUpdateCount { ReplaceRow(databaseName, tableName, rowID, values) }
  }

  def selectRows(databaseName: String,
                 tableName: String,
                 fields: Seq[Expression],
                 where: KeyValues,
                 groupBy: Seq[SQLField] = Nil,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int]): Future[QueryResult] = {
    asResultSet { SelectRows(databaseName, tableName, fields, where, groupBy, orderBy, limit) }
  }

  def truncateTable(databaseName: String, tableName: String): Future[UpdateCount] = {
    asUpdateCount { TruncateTable(databaseName, tableName) }
  }

  def unlockRow(databaseName: String, tableName: String, rowID: ROWID, lockID: String): Future[UpdateCount] = {
    asLockUpdated { UnlockRow(databaseName, tableName, rowID, lockID) } map(lu => UpdateCount(count = (!lu.isLocked).toInt, __id = Some(rowID)))
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): Future[UpdateCount] = {
    asUpdateCount { UpdateField(databaseName, tableName, rowID, columnID, value) }
  }

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: Seq[(String, Expression)]): Future[UpdateCount] = {
    asUpdateCount { UpdateRow(databaseName, tableName, rowID, values) }
  }

  def updateRows(databaseName: String, tableName: String, values: Seq[(String, Expression)], condition: KeyValues, limit: Option[Int] = None): Future[UpdateCount] = {
    asUpdateCount { UpdateRows(databaseName, tableName, values, condition, limit) }
  }

  private def ?(message: DatabaseIORequest): Future[DatabaseIOResponse] = {
    (actorPool ? message).mapTo[DatabaseIOResponse]
  }

  private def asLockUpdated(command: DatabaseIORequest): Future[LockUpdated] = {
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case cmd: LockUpdated => cmd
      case response => throw UnhandledCommandException(command, response)
    }
  }

  private def asResultSet(command: DatabaseIORequest): Future[QueryResult] = {
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case QueryResultRetrieved(queryResult) => queryResult
      case response => throw UnhandledCommandException(command, response)
    }
  }

  private def asRows(command: DatabaseIORequest): Future[Seq[Row]] = {
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case RowsRetrieved(rows) => rows
      case response => throw UnhandledCommandException(command, response)
    }
  }

  private def asUpdateCount(command: DatabaseIORequest): Future[UpdateCount] = {
    this ? command map {
      case FailureOccurred(command, cause) => throw FailedCommandException(command, cause)
      case RowUpdated(rowID, isSuccess) => UpdateCount(count = isSuccess.toInt, __id = Some(rowID))
      case RowsUpdated(count) => UpdateCount(count = count)
      case response => throw UnhandledCommandException(command, response)
    }
  }

}

/**
 * Query Processor Companion
 */
object QueryProcessor {
  private val logger = LoggerFactory.getLogger(getClass)
  private val databaseWorkers = TrieMap[String, ActorRef]()
  private val tableWorkers = TrieMap[(String, String), ActorRef]()
  private val vTableWorkers = TrieMap[(String, String), ActorRef]()

  /**
   * Sends the command to the caller
   * @param command the [[DatabaseIORequest command]]
   * @param caller  the [[ActorRef caller]]
   * @param block   the executable code block
   * @param f       the transformation function
   * @tparam A the response object type
   */
  def invoke[A](command: DatabaseIORequest, caller: ActorRef)(block: => A)(f: (ActorRef, A) => Unit): Unit = {
    try f(caller, block) catch {
      case e: Throwable =>
        caller ! FailureOccurred(command, e)
    }
  }

  /**
    * Determine which type of actor (database, tables, views) should handle the request
    * @param request the [[DatabaseIORequest request]]
    * @param system  the [[ActorSystem actor system]]
    * @return the [[ActorRef]]
    */
  private def determineWorker(request: DatabaseIORequest)(implicit system: ActorSystem): ActorRef = {

    def launchDW(command: DatabaseIORequest): ActorRef = {
      import command.databaseName
      databaseWorkers.getOrElseUpdate(databaseName, system.actorOf(Props(new DatabaseCPU(databaseName))))
    }

    def launchTW(command: TableIORequest): ActorRef = {
      import command.{databaseName, tableName}
      tableWorkers.getOrElseUpdate(databaseName -> tableName, system.actorOf(Props(new TableCPU(databaseName, tableName))))
    }

    def launchVTW(command: TableIORequest): ActorRef = {
      import command.{databaseName, tableName}
      vTableWorkers.getOrElseUpdate(databaseName -> tableName, system.actorOf(Props(new VirtualTableCPU(databaseName, tableName))))
    }

    request match {
      case cmd: FindRows => if (VirtualTableFile.isVirtualTable(cmd)) launchVTW(cmd) else launchTW(cmd)
      case cmd: SelectRows => if (VirtualTableFile.isVirtualTable(cmd)) launchVTW(cmd) else launchTW(cmd)
      case cmd: VirtualTableIORequest => launchVTW(cmd)
      case cmd: TableIORequest => launchTW(cmd)
      case cmd => launchDW(cmd)
    }
  }

  /**
   * Command Routing CPU
   * @param requestTimeout the [[FiniteDuration request timeout]]
   */
  class CommandRoutingCPU(requestTimeout: FiniteDuration) extends Actor {
    import context.{dispatcher, system}
    private implicit val timeout: Timeout = requestTimeout

    override def receive: Receive = {
      case command: DropTable =>
        processRequest(caller = sender(), command) onComplete { _ =>
          // kill the actor whom is responsible for the table
          tableWorkers.remove(command.databaseName -> command.tableName).foreach(_ ! PoisonPill)
        }
      case command: DropView =>
        processRequest(caller = sender(), command) onComplete { _ =>
          // kill the actor whom is responsible for the virtual table
          vTableWorkers.remove(command.databaseName -> command.tableName).foreach(_ ! PoisonPill)
        }
      case command: DatabaseIORequest => processRequest(caller = sender(), command)
      case message =>
        logger.error(s"Unhandled routing message $message")
        unhandled(message)
    }

    private def processRequest(caller: ActorRef, command: DatabaseIORequest): Future[DatabaseIOResponse] = {
      try {
        // perform the remote command
        val worker = determineWorker(command)
        val promise = (worker ? command).mapTo[DatabaseIOResponse]
        promise onComplete {
          case Success(response) => caller ! response
          case Failure(cause) => caller ! FailureOccurred(command, cause)
        }
        promise
      } catch {
        case e: Throwable =>
          caller ! FailureOccurred(command, e)
          Future.failed(e)
      }
    }

  }

  /**
   * Database Command Processing Unit Actor
   * @param databaseName the database name
   */
  class DatabaseCPU(databaseName: String) extends Actor {
    private lazy val databaseFile = DatabaseFile(databaseName)

    override def receive: Receive = {
      case cmd: GetDatabaseMetrics =>
        invoke(cmd, sender())(databaseFile.getDatabaseMetrics) { case (caller, metrics) => caller ! DatabaseMetricsRetrieved(metrics) }
      case message =>
        logger.error(s"Unhandled D-CPU processing message $message")
        unhandled(message)
    }
  }

  /**
   * Table Command Processing Unit Actor
   * @param databaseName the database name
   * @param tableName    the table name
   */
  class TableCPU(databaseName: String, tableName: String) extends Actor {
    private val locks = TrieMap[ROWID, String]()
    private lazy val table = TableFile(databaseName, tableName)

    override def receive: Receive = {
      case cmd@CreateIndex(_, _, indexColumn) =>
        invoke(cmd, sender())(table.createIndex(indexColumn)) { case (caller, _) => caller ! RowsUpdated(1) }
      case cmd@CreateTable(_, tableName, columns) =>
        invoke(cmd, sender())(TableFile.createTable(databaseName, tableName, columns.map(_.toColumn))) { case (caller, _) => caller ! RowsUpdated(1) }
      case cmd@DeleteField(_, _, rowID, columnID) =>
        invoke(cmd, sender())(table.deleteField(rowID, columnID)) { case (caller, _) => caller ! RowsUpdated(1) }
      case cmd@DeleteRange(_, _, start, length) =>
        invoke(cmd, sender())(table.deleteRange(start, length)) { case (caller, n) => caller ! RowsUpdated(n) }
      case cmd@DeleteRow(_, _, rowID) =>
        invoke(cmd, sender())(table.deleteRow(rowID)) { case (caller, _) => caller ! RowUpdated(rowID, isSuccess = true) }
      case cmd@DeleteRows(_, _, condition, limit) =>
        invoke(cmd, sender())(table.deleteRows(condition, limit)) { case (caller, n) => caller ! RowsUpdated(n) }
      case cmd@DropTable(_, tableName, ifExists) =>
        invoke(cmd, sender())(TableFile.dropTable(databaseName, tableName, ifExists)) { case (caller, isDropped) => caller ! RowsUpdated(count = isDropped.toInt) }
      case cmd@FetchAndReplace(_, _, rowID, f) =>
        invoke(cmd, sender())(table.fetchAndReplace(rowID)(f)) { case (caller, row) => caller ! RowsRetrieved(Seq(row)) }
      case cmd@FindRows(_, _, condition, limit) =>
        invoke(cmd, sender())(table.getRows(condition, limit)) { case (caller, rows) => caller ! RowsRetrieved(rows.use(_.toList)) }
      case cmd@GetField(_, _, rowID, columnID) =>
        invoke(cmd, sender())(table.getField(rowID, columnID)) { case (caller, field) => caller ! FieldRetrieved(field) }
      case cmd@GetRange(_, _, start, length) =>
        invoke(cmd, sender())(table.getRange(start, length)) { case (caller, rows) => caller ! RowsRetrieved(rows.use(_.toList)) }
      case cmd@GetRow(_, _, rowID) =>
        invoke(cmd, sender())(table.getRow(rowID)) { case (caller, row_?) => caller ! RowsRetrieved(row_?.toSeq) }
      case cmd: GetTableLength =>
        invoke(cmd, sender())(table.device.length) { case (caller, n) => caller ! RowsUpdated(n) }
      case cmd: GetTableMetrics =>
        invoke(cmd, sender())(table.getTableMetrics) { case (caller, metrics) => caller ! TableMetricsRetrieved(metrics) }
      case cmd@InsertRow(_, _, row) =>
        invoke(cmd, sender())(table.insertRow(row)) { case (caller, _id) => caller ! RowUpdated(_id, isSuccess = true) }
      case cmd@InsertRows(_, _, columns, rows) =>
        implicit val scope: Scope = Scope()
        val values = rows.map(_.map { expr => evaluate(expr).value.orNull })
        invoke(cmd, sender())(table.insertRows(columns, values)) { case (caller, n) => caller ! RowsUpdated(n.length) }
      case cmd@LockRow(_, _, rowID) => lockRow(cmd, rowID)
      case cmd@ReplaceRange(_, _, start, length, row) =>
        invoke(cmd, sender())(table.replaceRange(start, length, row)) { case (caller, _) => caller ! RowsUpdated(length) }
      case cmd@ReplaceRow(_, _, rowID, row) =>
        invoke(cmd, sender())(table.replaceRow(rowID, row)) { case (caller, _) => caller ! RowUpdated(rowID, isSuccess = true) }
      case cmd@SelectRows(_, _, fields, where, groupBy, orderBy, limit) =>
        invoke(cmd, sender())(table.selectRows(fields, where, groupBy, orderBy, limit)) { case (caller, result) => caller ! QueryResultRetrieved(result.use(QueryResult.toQueryResult(databaseName, tableName, _))) }
      case cmd: TruncateTable =>
        invoke(cmd, sender())(table.truncate()) { case (caller, n) => caller ! RowsUpdated(n) }
      case cmd@UnlockRow(_, _, rowID, lockID) => unlockRow(cmd, rowID, lockID)
      case cmd@UpdateField(_, _, rowID, columnID, value) =>
        invoke(cmd, sender())(table.updateField(rowID, columnID, value)) { case (caller, _) => caller ! RowUpdated(rowID, isSuccess = true) }
      case cmd@UpdateRow(_, _, rowID, changes) =>
        implicit val scope: Scope = Scope()
        val row = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
        invoke(cmd, sender())(table.updateRow(rowID, row)) { case (caller, _) => caller ! RowUpdated(rowID, isSuccess = true) }
      case cmd@UpdateRows(_, _, changes, condition, limit) =>
        implicit val scope: Scope = Scope()
        val values = KeyValues(changes.map { case (name, expr) => (name, evaluate(expr)) }: _*)
        invoke(cmd, sender())(table.updateRows(values, condition, limit)) { case (caller, n) => caller ! RowsUpdated(n) }
      case message =>
        logger.error(s"Unhandled T-CPU processing message $message")
        unhandled(message)
    }

    override def postStop(): Unit = {
      logger.info(s"Table actor '$databaseName.$tableName' was shutdown")
      super.postStop()
    }

    private def lockRow(cmd: LockRow, rowID: ROWID): Unit = {
      val newLockID = UUID.randomUUID().toString
      invoke(cmd, sender())(table.lockRow(rowID)) { case (caller, _) =>
        locks(rowID) = newLockID
        caller ! LockUpdated(rowID, newLockID, isLocked = true)
      }
    }

    private def unlockRow(cmd: UnlockRow, rowID: ROWID, lockID: String): Unit = {
      invoke(cmd, sender())(table.unlockRow(rowID)) { case (caller, _) =>
        locks.remove(rowID)
        caller ! LockUpdated(rowID, lockID, isLocked = false)
      }
    }

  }

  /**
    * Virtual Table Command Processing Unit Actor
    * @param databaseName the database name
    * @param viewName     the view name
    */
  class VirtualTableCPU(databaseName: String, viewName: String) extends Actor {
    private lazy val vTable = VirtualTableFile(databaseName, viewName)

    override def receive: Receive = {
      case cmd@CreateView(_, _, queryString, ifNotExists) =>
        invoke(cmd, sender())(VirtualTableFile.createView(databaseName, viewName, queryString, ifNotExists)) { case (caller, _) => caller ! RowsUpdated(1) }
      case cmd@DropView(_, _, ifExists) =>
        invoke(cmd, sender())(VirtualTableFile.dropView(databaseName, viewName, ifExists)) { case (caller, _) => caller ! RowsUpdated(1) }
      case cmd@FindRows(_, _, condition, limit) =>
        invoke(cmd, sender())(vTable.getRows(condition, limit)) { case (caller, rows) => caller ! RowsRetrieved(rows.use(_.toList)) }
      case cmd@SelectRows(_, _, fields, where, groupBy, orderBy, limit) =>
        invoke(cmd, sender())(vTable.selectRows(fields, where, groupBy, orderBy, limit)) { case (caller, result) => caller ! QueryResultRetrieved(result.use(QueryResult.toQueryResult(databaseName, viewName, _))) }
      case message =>
        logger.error(s"Unhandled VT-CPU processing message $message")
        unhandled(message)
    }

    override def postStop(): Unit = {
      logger.info(s"Virtual Table actor '$databaseName.$viewName' was shutdown")
      super.postStop()
    }
  }

  object commands {

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      COMMAND PROTOTYPES
    /////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Represents a Database I/O Request
     */
    sealed trait DatabaseIORequest {
      def databaseName: String
    }

    /**
     * Represents a Table I/O Request
     */
    sealed trait TableIORequest extends DatabaseIORequest {
      def tableName: String
    }

    /**
      * Represents a Virtual Table I/O Request
      */
    sealed trait VirtualTableIORequest extends TableIORequest

    /**
     * Represents a Database I/O Response
     */
    sealed trait DatabaseIOResponse

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      READ-ONLY COMMANDS
    /////////////////////////////////////////////////////////////////////////////////////////////////

    case class FindRows(databaseName: String,
                        tableName: String,
                        condition: KeyValues,
                        limit: Option[Int] = None) extends TableIORequest

    case class GetDatabaseMetrics(databaseName: String) extends DatabaseIORequest

    case class GetField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int) extends TableIORequest

    case class GetRange(databaseName: String, tableName: String, start: ROWID, length: Int) extends TableIORequest

    case class GetRow(databaseName: String, tableName: String, rowID: ROWID) extends TableIORequest

    case class GetTableLength(databaseName: String, tableName: String) extends TableIORequest

    case class GetTableMetrics(databaseName: String, tableName: String) extends TableIORequest

    case class SelectRows(databaseName: String,
                          tableName: String,
                          fields: Seq[Expression],
                          where: KeyValues,
                          groupBy: Seq[SQLField] = Nil,
                          orderBy: Seq[OrderColumn] = Nil,
                          limit: Option[Int] = None) extends TableIORequest

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      TABLE MUTATION COMMANDS
    /////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Represents a Table Update Request
     */
    sealed trait TableUpdateRequest extends TableIORequest

    case class CreateIndex(databaseName: String, tableName: String, indexColumnName: String) extends TableUpdateRequest

    case class CreateTable(databaseName: String, tableName: String, columns: Seq[TableColumn]) extends TableUpdateRequest

    case class DeleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int) extends TableUpdateRequest

    case class DeleteRange(databaseName: String, tableName: String, start: ROWID, length: Int) extends TableUpdateRequest

    case class DeleteRow(databaseName: String, tableName: String, rowID: ROWID) extends TableUpdateRequest

    case class DeleteRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int]) extends TableUpdateRequest

    case class DropTable(databaseName: String, tableName: String, ifExists: Boolean) extends TableUpdateRequest

    case class FetchAndReplace(databaseName: String, tableName: String, rowID: ROWID, f: KeyValues => KeyValues) extends TableUpdateRequest

    case class InsertRow(databaseName: String, tableName: String, row: KeyValues) extends TableUpdateRequest

    case class InsertRows(databaseName: String, tableName: String, columns: Seq[String], values: List[Insert.DataRow]) extends TableUpdateRequest

    case class InsertSelect(databaseName: String, tableName: String, select: SelectRows) extends TableUpdateRequest

    case class LockRow(databaseName: String, tableName: String, rowID: ROWID) extends TableUpdateRequest

    case class ReplaceRow(databaseName: String, tableName: String, rowID: ROWID, row: KeyValues) extends TableUpdateRequest

    case class ReplaceRange(databaseName: String, tableName: String, start: ROWID, length: Int, row: KeyValues) extends TableUpdateRequest

    case class TruncateTable(databaseName: String, tableName: String) extends TableUpdateRequest

    case class UnlockRow(databaseName: String, tableName: String, rowID: ROWID, lockID: String) extends TableUpdateRequest

    case class UpdateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]) extends TableUpdateRequest

    case class UpdateRow(databaseName: String, tableName: String, rowID: ROWID, changes: Seq[(String, Expression)]) extends TableUpdateRequest

    case class UpdateRows(databaseName: String, tableName: String, changes: Seq[(String, Expression)], condition: KeyValues, limit: Option[Int]) extends TableUpdateRequest

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      VIRTUAL TABLE MUTATIONS
    /////////////////////////////////////////////////////////////////////////////////////////////////

    case class CreateView(databaseName: String, tableName: String, queryString: String, ifNotExists: Boolean) extends VirtualTableIORequest

    case class DropView(databaseName: String, tableName: String, ifExists: Boolean) extends VirtualTableIORequest

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      RESPONSE COMMANDS
    /////////////////////////////////////////////////////////////////////////////////////////////////

    case class DatabaseMetricsRetrieved(metrics: DatabaseMetrics) extends DatabaseIOResponse

    case class FailureOccurred(command: DatabaseIORequest, cause: Throwable) extends DatabaseIOResponse

    case class FieldRetrieved(field: Field) extends DatabaseIOResponse

    case class LockUpdated(rowID: ROWID, lockID: String, isLocked: Boolean) extends DatabaseIOResponse

    case class QueryResultRetrieved(result: QueryResult) extends DatabaseIOResponse

    case class RowsRetrieved(rows: Seq[Row]) extends DatabaseIOResponse

    case class RowsUpdated(count: Int) extends DatabaseIOResponse

    case class RowUpdated(rowID: ROWID, isSuccess: Boolean) extends DatabaseIOResponse

    case class TableMetricsRetrieved(metrics: TableMetrics) extends DatabaseIOResponse

  }

}

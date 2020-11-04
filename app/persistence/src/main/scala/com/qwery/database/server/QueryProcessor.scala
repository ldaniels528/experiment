package com.qwery.database.server

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Scheduler}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.qwery.database.server.InvokableProcessor.implicits.InvokableFacade
import com.qwery.database.server.QueryProcessor.TableCommandRoutingActor
import com.qwery.database.server.QueryProcessor.commands._
import com.qwery.database.server.TableService.{QueryResult, TableColumn}
import com.qwery.database.types.QxAny
import com.qwery.database.{Field, QweryFiles, ROWID, Row}
import com.qwery.language.SQLLanguageParser

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
 * Query Processor
 * @param routingActors  the number of command routing actors
 * @param requestTimeout the [[FiniteDuration request timeout]]
 */
class QueryProcessor(routingActors: Int = 5, requestTimeout: FiniteDuration = 5.seconds) {
  private val actorSystem: ActorSystem = ActorSystem(name = "QueryProcessor")
  private val actorPool: ActorRef = actorSystem.actorOf(Props(new TableCommandRoutingActor(requestTimeout))
    .withRouter(RoundRobinPool(nrOfInstances = routingActors)))

  implicit val _timeout: Timeout = requestTimeout
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val scheduler: Scheduler = actorSystem.scheduler

  def !(message: TableIORequest): Unit = actorPool ! message

  def ?(message: DatabaseIORequest): Future[DatabaseIOResponse] = (actorPool ? message).mapTo[DatabaseIOResponse]

  def createTable(databaseName: String, tableName: String, columns: Seq[TableColumn]): Future[QueryResult] = {
    mutateOne { CreateTable(databaseName, tableName, columns) }
  }

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Future[QueryResult] = {
    mutateOne { DeleteField(databaseName, tableName, rowID, columnID) }
  }

  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): Future[QueryResult] = {
    mutateOne { DeleteRow(databaseName, tableName, rowID) }
  }

  def dropTable(databaseName: String, tableName: String): Future[QueryResult] = {
    mutateOne { DropTable(databaseName, tableName) }
  }

  def executeQuery(databaseName: String, sql: String): Future[QueryResult] = {
    retrieveRows { ExecuteQuery(databaseName, sql) }
  }

  def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Future[Field] = {
    val command = GetField(databaseName, tableName, rowID, columnID)
    this ? command map {
      case FailureResponse(cause, _, msec) => throw new RuntimeException(s"Request '$command' failed after $msec", cause)
      case FieldRetrieved(field, _) => field
      case other => throw new RuntimeException(s"After a '$command' an unhandled message '$other' was received")
    }
  }

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Future[Option[Row]] = {
    retrieveRow { GetRow(databaseName, tableName, rowID) }
  }

  def getTableLength(databaseName: String, tableName: String): Future[QueryResult] = {
    mutateOne { GetTableLength(databaseName, tableName) }
  }

  def insertRow(databaseName: String, tableName: String, values: TupleSet): Future[QueryResult] = {
    mutateOne { InsertRow(databaseName, tableName, values) }
  }

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): Future[QueryResult] = {
    mutateOne { ReplaceRow(databaseName, tableName, rowID, values) }
  }

  def truncateTable(databaseName: String, tableName: String): Future[QueryResult] = {
    mutateOne { TruncateTable(databaseName, tableName) }
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: QxAny): Future[QueryResult] = {
    mutateOne { UpdateField(databaseName, tableName, rowID, columnID, value) }
  }

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): Future[QueryResult] = {
    mutateOne { UpdateRow(databaseName, tableName, rowID, values) }
  }

  private def mutateOne(command: TableIORequest): Future[QueryResult] = {
    this ? command map {
      case FailureResponse(cause, _, msec) => throw new RuntimeException(s"Request '$command' failed after $msec", cause)
      case RowUpdated(rowID, msec) => QueryResult(command.databaseName, command.tableName, count = 1, __id = Some(rowID), responseTime = msec)
      case RowsUpdated(count, msec) => QueryResult(command.databaseName, command.tableName, count = count, responseTime = msec)
      case other => throw new RuntimeException(s"After a '$command' an unhandled message '$other' was received")
    }
  }

  private def retrieveRow(command: TableIORequest): Future[Option[Row]] = {
    this ? command map {
      case FailureResponse(cause, _, msec) => throw new RuntimeException(s"Request '$command' failed after $msec", cause)
      case RowRetrieved(row_?, msec) => row_?
      case other => throw new RuntimeException(s"After a '$command' an unhandled message '$other' was received")
    }
  }

  private def retrieveRows(command: DatabaseIORequest): Future[QueryResult] = {
    this ? command map {
      case FailureResponse(cause, _, msec) => throw new RuntimeException(s"Request '$command' failed after $msec", cause)
      case QueryResponse(rows, _) => rows
      //case RowRetrieved(rows, msec) => QueryResult(command.databaseName,command.tableName, rows = rows.toSeq.map(_.fields.map(_.typedValue.value)), responseTime = msec)
      case other => throw new RuntimeException(s"After a '$command' an unhandled message '$other' was received")
    }
  }

}

/**
 * Query Processor Companion
 */
object QueryProcessor {
  private val workers = TrieMap[(String, String), ActorRef]()

  /**
   * Table Command Routing Actor
   * @param requestTimeout the [[FiniteDuration request timeout]]
   */
  class TableCommandRoutingActor(requestTimeout: FiniteDuration = 5.seconds) extends Actor with ActorLogging {
    import context.dispatcher
    implicit val timeout: Timeout = requestTimeout

    override def receive: Receive = {
      case command@ExecuteQuery(_, sql) =>
        makeRequest(caller = sender(), command, tableName = SQLLanguageParser.parse(sql).extractTableName)
      case command: TableIORequest =>
        makeRequest(caller = sender(), command, tableName = command.tableName)
      case message =>
        log.error(s"Unhandled routing message $message")
        unhandled(message)
    }

    private def getOrCreateWorker(databaseName: String, tableName: String): ActorRef = {
      workers.getOrElseUpdate((databaseName, tableName), context.system.actorOf(Props(new TableCommandProcessingActor(databaseName, tableName))))
    }

    private def makeRequest(caller: ActorRef, command: DatabaseIORequest, tableName: String): Unit = {
      val worker = getOrCreateWorker(command.databaseName, tableName)
      val startTime = System.nanoTime()
      (worker ? command).mapTo[DatabaseIOResponse] onComplete {
        // TODO intercept and wrap non-DatabaseIORequest instances
        case Success(response) => caller ! response
        case Failure(e) => caller ! FailureResponse(e, command, responseTimeMsec = (System.nanoTime() - startTime) / 1e+6)
      }
    }
  }

  /**
   * Table Command Processing Actor
   * @param databaseName the database name
   * @param tableName    the table name
   */
  class TableCommandProcessingActor(databaseName: String, tableName: String) extends Actor with ActorLogging {
    private val table: TableFile = QweryFiles.getTableFile(databaseName, tableName)

    override def receive: Receive = {
      case cmd@CreateTable(_, _, columns) =>
        invoke(cmd, sender())(TableFile.createTable(databaseName, tableName, columns.map(_.toColumn))) { case (caller, _, msec) => caller ! RowsUpdated(1, msec) }
      case cmd@DeleteField(_, _, rowID, columnID) =>
        invoke(cmd, sender())(table.deleteField(rowID, columnID)) { case (caller, b, msec) => caller ! RowsUpdated(if (b) 1 else 0, msec) }
      case cmd@DeleteRow(_, _, rowID) =>
        invoke(cmd, sender())(table.deleteRow(rowID)) { case (caller, n, msec) => caller ! RowUpdated(n, msec) }
      case cmd: DropTable =>
        invoke(cmd, sender())(TableFile.dropTable(databaseName, tableName)) { case (caller, b, msec) => caller ! RowsUpdated(if (b) 1 else 0, msec) }
      case cmd@ExecuteQuery(_, sql) =>
        invoke(cmd, sender())(TableFile.executeQuery(databaseName, sql)) { case (caller, rows, msec) => caller ! QueryResponse(rows, msec) }
      case cmd@GetField(_, _, rowID, columnID) =>
        invoke(cmd, sender())(table.getField(rowID, columnID)) { case (caller, field, msec) => caller ! FieldRetrieved(field, msec) }
      case cmd@GetRow(_, _, rowID) =>
        invoke(cmd, sender())(table.get(rowID)) { case (caller, row_?, msec) => caller ! RowRetrieved(row_?, msec) }
      case cmd: GetTableLength =>
        invoke(cmd, sender())(table.device.length) { case (caller, n, msec) => caller ! RowsUpdated(n, msec) }
      case cmd@InsertRow(_, _, row) =>
        invoke(cmd, sender())(table.insertRow(row)) { case (caller, rowID, msec) => caller ! RowUpdated(rowID, msec) }
      case cmd@ReplaceRow(_, _, rowID, row) =>
        invoke(cmd, sender())(table.replaceRow(rowID, row)) { case (caller, _, msec) => caller ! RowUpdated(rowID, msec) }
      case cmd: TruncateTable =>
        invoke(cmd, sender())(table.truncate()) { case (caller, n, msec) => caller ! RowsUpdated(n, msec) }
      case cmd@UpdateField(_, _, rowID, columnID, value) =>
        invoke(cmd, sender())(table.updateField(rowID, columnID, value.value)) { case (caller, _, msec) => caller ! RowUpdated(rowID, msec) }
      case cmd@UpdateRow(_, _, rowID, row) =>
        invoke(cmd, sender())(table.updateRow(rowID, row)) { case (caller, _, msec) => caller ! RowUpdated(rowID, msec) }
      case message =>
        log.error(s"Unhandled processing message $message")
        unhandled(message)
    }

    private def invoke[A](command: DatabaseIORequest, caller: ActorRef)(block: => A)(f: (ActorRef, A, Double) => Unit): Unit = {
      val startTime = System.nanoTime()
      val elapsedTime = () => (System.nanoTime() - startTime) / 1e+6
      Try(block) match {
        case Success(rowID) => f(caller, rowID, elapsedTime())
        case Failure(e) =>
          caller ! FailureResponse(e, command, elapsedTime())
          None
      }
    }

  }

  protected[server] object commands {

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      REQUESTS
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

    case class CreateTable(databaseName: String, tableName: String, columns: Seq[TableColumn]) extends TableIORequest

    case class DeleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int) extends TableIORequest

    case class DeleteRow(databaseName: String, tableName: String, rowID: ROWID) extends TableIORequest

    case class DropTable(databaseName: String, tableName: String) extends TableIORequest

    case class ExecuteQuery(databaseName: String, sql: String) extends DatabaseIORequest

    case class GetField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int) extends TableIORequest

    case class GetRow(databaseName: String, tableName: String, rowID: ROWID) extends TableIORequest

    case class GetTableLength(databaseName: String, tableName: String) extends TableIORequest

    case class InsertRow(databaseName: String, tableName: String, row: TupleSet) extends TableIORequest

    case class ReplaceRow(databaseName: String, tableName: String, rowID: ROWID, row: TupleSet) extends TableIORequest

    case class TruncateTable(databaseName: String, tableName: String) extends TableIORequest

    case class UpdateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: QxAny) extends TableIORequest

    case class UpdateRow(databaseName: String, tableName: String, rowID: ROWID, row: TupleSet) extends TableIORequest

    /////////////////////////////////////////////////////////////////////////////////////////////////
    //      RESPONSES
    /////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Represents a Database I/O Response
     */
    sealed trait DatabaseIOResponse {

      def responseTimeMsec: Double

    }

    case class FailureResponse(cause: Throwable, command: DatabaseIORequest, responseTimeMsec: Double) extends DatabaseIOResponse

    case class FieldRetrieved(field: Field, responseTimeMsec: Double) extends DatabaseIOResponse

    case class QueryResponse(rows: QueryResult, responseTimeMsec: Double) extends DatabaseIOResponse

    case class RowUpdated(rowID: ROWID, responseTimeMsec: Double) extends DatabaseIOResponse

    case class RowRetrieved(row: Option[Row], responseTimeMsec: Double) extends DatabaseIOResponse

    case class RowsUpdated(count: Int, responseTimeMsec: Double) extends DatabaseIOResponse

  }

}

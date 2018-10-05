package com.qwery.platform.flink

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.models.ColumnTypes._
import com.qwery.models.StorageFormats._
import com.qwery.models._
import com.qwery.models.expressions._
import com.qwery.platform.flink.FlinkQweryCompiler.Implicits._
import com.qwery.util.OptionHelper._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.Try

/**
  * Qwery Compiler for Apache Flink
  * @author lawrence.daniels@gmail.com
  */
trait FlinkQweryCompiler {

  /**
    * Compiles the given condition
    * @param condition the given [[Condition condition]]
    * @return the resulting [[FlinkInvokable column]]
    */
  @throws[IllegalArgumentException]
  def compile(condition: Condition)(implicit rc: FlinkQweryContext): String = condition.compile

  /**
    * Compiles the given expression
    * @param expression the given [[Expression expression]]
    * @return the resulting [[FlinkInvokable column]]
    */
  @throws[IllegalArgumentException]
  def compile(expression: Expression)(implicit rc: FlinkQweryContext): String = expression.compile

  /**
    * Compiles the given statement
    * @param statement the given [[Invokable statement]]
    * @return the resulting [[FlinkInvokable operation]]
    */
  @throws[IllegalArgumentException]
  def compile(statement: Invokable)(implicit rc: FlinkQweryContext): FlinkInvokable = statement.compile

  /**
    * Compiles the given statement
    * @param statement the given [[Invokable statement]]
    * @param args      the command line arguments
    * @return the resulting [[FlinkInvokable operation]]
    */
  @throws[IllegalArgumentException]
  def compileAndRun(statement: Invokable, args: Seq[String])(implicit rc: FlinkQweryContext): Unit = {
    statement.compile.execute(input = None)
    ()
  }

}

/**
  * Flink Qwery Compiler Companion
  * @author lawrence.daniels@gmail.com
  */
object FlinkQweryCompiler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val flinkTypeMapping = {
    import org.apache.flink.table.api.Types
    Map(
      BOOLEAN -> Types.BOOLEAN,
      DATE -> Types.SQL_DATE,
      DOUBLE -> Types.DOUBLE,
      INTEGER -> Types.INT,
      LONG -> Types.LONG,
      STRING -> Types.STRING)
  }

  /**
    * Returns the equivalent query operation to represent the given table or view
    * @param tableOrView the given [[TableLike table or view]]
    * @param rc          the implicit [[FlinkQweryContext]]
    * @return the [[DataFrame]]
    */
  def read(tableOrView: TableLike)(implicit rc: FlinkQweryContext): Option[DataFrame] = tableOrView match {
    case table: Table =>
      val df = table.inputFormat.orFail("The input format was not specified") match {
        case CSV =>
          val builder0 = CsvTableSource.builder
            .path(table.location)
            .ignoreFirstLine
            .fieldDelimiter(table.fieldDelimiter || ",")
            .quoteCharacter('"')
          val tableSource = table.columns.foldLeft(builder0) { case (build, column) =>
            build.field(column.name, flinkTypeMapping.getOrElse(column.`type`, die(s"Unsupported type ${column.`type`}")))
          }.build
          Try(rc.tableEnv.registerTableSource(table.name, tableSource))
          rc.tableEnv.scan(table.name)
        case format => die(s"Storage format $format is not supported for reading")
      }
      // rename the columns
      Option(df)
    case view: View => view.query.compile.execute(input = None)
    case unknown =>
      die(s"Unrecognized table type '$unknown' (${unknown.getClass.getName})")
  }

  /**
    * Writes the source data frame to the given target
    * @param source      the source [[DataFrame]]
    * @param destination the [[Location destination table or location]]
    * @param append      indicates whether the destination should be appended (or conversely overwritten)
    * @param rc          the implicit [[FlinkQweryContext]]
    */
  def write(source: DataFrame, destination: Location, append: Boolean)(implicit rc: FlinkQweryContext): Unit =
    write(source, destination = rc.getTableOrView(destination), append)

  /**
    * Writes the source data frame to the given target
    * @param source      the source [[DataFrame]]
    * @param destination the [[TableLike destination table or view]]
    * @param append      indicates whether the destination should be appended (or conversely overwritten)
    * @param rc          the implicit [[FlinkQweryContext]]
    */
  def write(source: DataFrame, destination: TableLike, append: Boolean)(implicit rc: FlinkQweryContext): Unit = {
    import com.qwery.util.OptionHelper.Implicits.Risky._

    val writeMode = if (append) WriteMode.NO_OVERWRITE else WriteMode.OVERWRITE
    destination match {
      case table: Table =>
        table.outputFormat.orFail("The output format was not specified") match {
          case CSV =>
            val sink = new CsvTableSink(path = table.location, fieldDelim = table.fieldDelimiter || ",", numFiles = None, writeMode = writeMode)
            source.foreach(_.writeToSink(sink))
          case format =>
            die(s"Storage format $format is not supported for writing")
        }
      case view: View =>
        die(s"View '${view.name}' cannot be modified")
    }
  }

  /**
    * Query table/view reference for Flink
    * @param name the name of the table
    */
  case class ReadTableOrViewByReference(name: String, alias: Option[String]) extends FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = Table(name).getQuery
  }

  /**
    * Registers a table or view for use with Flink
    * @param tableOrView the [[TableLike table or view]]
    */
  case class RegisterTableOrView(tableOrView: TableLike) extends FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      logger.info(s"Registering '${tableOrView.name}' as a ${if (tableOrView.isInstanceOf[Table]) "table" else "view"}...")
      rc += tableOrView
      input
    }
  }

  /**
    * Implicit conversions
    * @author lawrence.daniels@gmail.com
    */
  object Implicits {

    final implicit class FlinkConditionCompiler(val condition: Condition) extends AnyVal {
      @inline def compile(implicit rc: FlinkQweryContext): String = condition match {
        case AND(a, b) => s"${a.compile} AND ${b.compile}"
        case EQ(a, b) => s"${a.compile} = ${b.compile}"
        case GE(a, b) => s"${a.compile} >= ${b.compile}"
        case GT(a, b) => s"${a.compile} > ${b.compile}"
        case IsNotNull(c) => s"${c.compile} IS NOT NULL"
        case IsNull(c) => s"${c.compile} IS NULL"
        case LE(a, b) => s"${a.compile} <= ${b.compile}"
        case LIKE(a, b) => s"${a.compile} LIKE ${b.asString}"
        case LT(a, b) => s"${a.compile} < ${b.compile}"
        case NE(a, b) => s"${a.compile} <> ${b.compile}"
        case NOT(c) => s"NOT ${c.compile}"
        case OR(a, b) => s"${a.compile} OR ${b.compile}"
        case RLIKE(a, b) => s"${a.compile} RLIKE ${b.asString}"
        case unknown => die(s"Unhandled condition '$unknown'")
      }
    }

    final implicit class FlinkExpressionCompiler(val expression: Expression) extends AnyVal {
      @inline def compile(implicit rc: FlinkQweryContext): String = expression match {
        case Add(a, b) => a.compile + b.compile
        case ref@BasicField(name) => ref.alias.map(theAlias => s"$name AS $theAlias") || name
        case Divide(a, b) => a.compile + b.compile
        case Literal(value) => value.asInstanceOf[Object] match {
          case s: String => s"'$s'"
          case n: Number => n.doubleValue().toString
          case v => die(s"Unsupported data type '$v' (${v.getClass.getSimpleName})")
        }
        case Modulo(a, b) => s"${a.compile} % ${b.compile}"
        case Multiply(a, b) => s"${a.compile} * ${b.compile}"
        case Pow(a, b) => s"${a.compile} ** ${b.compile}"
        case Subtract(a, b) => s"${a.compile} - ${b.compile}"
        case unknown => die(s"Unhandled expression '$unknown'")
      }
    }

    final implicit class FlinkInvokableCompiler(val invokable: Invokable) extends AnyVal {
      def compile(implicit rc: FlinkQweryContext): FlinkInvokable = invokable match {
        case Create(tableOrView: TableLike) => RegisterTableOrView(tableOrView)
        case Console.Debug(text) => FlinkConsole.debug(text)
        case Console.Error(text) => FlinkConsole.error(text)
        case Console.Info(text) => FlinkConsole.info(text)
        case Console.Log(text) => FlinkConsole.log(text)
        case Console.Print(text) => FlinkConsole.print(text)
        case Console.Warn(text) => FlinkConsole.warn(text)
        case Include(paths) => incorporateSources(paths)
        case Insert(destination, source, fields) =>
          FlinkInsert(destination = destination.compile, fields = fields, source = source match {
            case Insert.Values(values) => FlinkInsert.Spout(values, target = Option(destination.target))
            case op => op.compile
          })
        case Insert.Into(target) => FlinkInsert.Sink(target = target, append = true)
        case Insert.Overwrite(target) => FlinkInsert.Sink(target = target, append = false)
        case Insert.Values(values) => FlinkInsert.Spout(values, target = None)
        case MainProgram(name, code, args, env, hive, streaming) => FlinkMainProgram(name, code.compile, args, env, hive, streaming)
        case ref@Select(columns, from, joins, groupBy, orderBy, where, limit) =>
          FlinkSelect(columns, from.map(_.compile), joins, groupBy, orderBy, where, limit, ref.alias)
        case Show(dataSet, limit) => FlinkShow(dataSet.compile, limit)
        case SQL(ops) => FlinkSQL(ops.map(_.compile))
        case ref@TableRef(name) => ReadTableOrViewByReference(name, ref.alias)
        case unknown => die(s"Unhandled operation '$unknown'")
      }

      private def incorporateSources(path: String)(implicit rc: FlinkQweryContext): FlinkInvokable = {
        val ops = Seq(path) map (new File(_).getCanonicalFile) map { file =>
          logger.info(s"Merging source file '${file.getAbsolutePath}'...")
          SQLLanguageParser.parse(file).compile
        }
        FlinkSQL(ops: _*)
      }
    }

    /**
      * Location Enrichment
      * @param reference the given [[Location reference]]
      */
    final implicit class LocationEnrichment(val reference: Location) extends AnyVal {

      /**
        * Attempts to retrieve the desired columns for this [[Location]]
        * @return the collection of [[Column columns]]
        */
      @inline def resolveColumns(implicit rc: FlinkQweryContext): List[Column] = rc.getTableOrView(reference).resolveColumns

      /**
        * Attempts to retrieve the desired data frame for this [[Location]]
        * @return the [[Table table]]
        */
      @inline def getQuery(implicit rc: FlinkQweryContext): Option[DataFrame] = reference match {
        case LocationRef(path) => throw new IllegalStateException("Writing to locations is not yet supported")
        case ref@TableRef(name) => read(rc.getTableOrView(name)) // TODO alias???
      }
    }

    /**
      * Table-Like Enrichment
      * @param tableLike the given [[TableLike table or view]]
      */
    final implicit class TableLikeEnrichment(val tableLike: TableLike) extends AnyVal {

      /**
        * Attempts to retrieve the desired columns for this [[TableLike]]
        * @return the collection of [[Column columns]]
        */
      @inline def resolveColumns(implicit rc: FlinkQweryContext): List[Column] = {
        tableLike match {
          case table: Table => table.columns
          case table => die(s"Could not resolve columns for '${table.name}'")
        }
      }
    }

  }

}
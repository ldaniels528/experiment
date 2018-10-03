package com.qwery.platform.spark

import java.io.File

import com.databricks.spark.avro._
import com.qwery.language.SQLLanguageParser
import com.qwery.models.ColumnTypes._
import com.qwery.models.StorageFormats._
import com.qwery.models._
import com.qwery.models.expressions.{Condition, Expression, RowSetVariableRef}
import com.qwery.platform.spark.SparkQweryCompiler.Implicits._
import com.qwery.platform.spark.SparkSelect.SparkJoin
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.functions.{callUDF, lit}
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SaveMode, Column => SparkColumn}
import org.slf4j.LoggerFactory

/**
  * Qwery Compiler for Apache Spark
  * @author lawrence.daniels@gmail.com
  */
trait SparkQweryCompiler {

  /**
    * Compiles the given condition
    * @param condition the given [[Condition condition]]
    * @return the resulting [[SparkColumn column]]
    */
  @throws[IllegalArgumentException]
  def compile(condition: Condition)(implicit rc: SparkQweryContext): SparkColumn = condition.compile

  /**
    * Compiles the given expression
    * @param expression the given [[Expression expression]]
    * @return the resulting [[SparkColumn column]]
    */
  @throws[IllegalArgumentException]
  def compile(expression: Expression)(implicit rc: SparkQweryContext): SparkColumn = expression.compile

  /**
    * Compiles the given statement
    * @param statement the given [[Invokable statement]]
    * @return the resulting [[SparkInvokable operation]]
    */
  @throws[IllegalArgumentException]
  def compile(statement: Invokable)(implicit rc: SparkQweryContext): SparkInvokable = statement.compile

  /**
    * Compiles the given statement
    * @param statement the given [[Invokable statement]]
    * @param args      the command line arguments
    * @return the resulting [[SparkInvokable operation]]
    */
  @throws[IllegalArgumentException]
  def compileAndRun(statement: Invokable, args: Seq[String])(implicit rc: SparkQweryContext): Unit = {
    val code = statement.compile
    code.execute(input = None)
    ()
  }

}

/**
  * Spark Qwery Compiler Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkQweryCompiler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val sparkTypeMapping = {
    import org.apache.spark.sql.types.DataTypes
    Map(
      BINARY -> DataTypes.BinaryType,
      BOOLEAN -> DataTypes.BooleanType,
      DATE -> DataTypes.DateType,
      DOUBLE -> DataTypes.DoubleType,
      INTEGER -> DataTypes.BooleanType,
      LONG -> DataTypes.LongType,
      STRING -> DataTypes.StringType)
  }

  /**
    * Returns the equivalent query operation to represent the given table or view
    * @param tableOrView the given [[TableLike table or view]]
    * @param rc          the implicit [[SparkQweryContext]]
    * @return the [[DataFrame]]
    */
  def read(tableOrView: TableLike)(implicit rc: SparkQweryContext): Option[DataFrame] = {
    import SparkQweryCompiler.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._
    tableOrView match {
      case SparkLogicalTable(name, columns, source) =>
        rc.createDataSet(columns, source).map { df => df.createOrReplaceTempView(name); df }
      case table: Table =>
        val reader = rc.spark.read.tableOptions(table)
        table.inputFormat.orFail("Table input format was not specified") match {
          case AVRO => reader.avro(table.location)
          case CSV => reader.schema(rc.createSchema(table.columns)).csv(table.location)
          case JDBC => reader.jdbc(table.location, table.name, table.properties || new java.util.Properties())
          case JSON => reader.json(table.location)
          case PARQUET => reader.parquet(table.location)
          case ORC => reader.orc(table.location)
          case format => die(s"Storage format $format is not supported for reading")
        }
      case view: View => view.query.compile.execute(input = None)
      case unknown => die(s"Unrecognized table type '$unknown' (${unknown.getClass.getName})")
    }
  }

  /**
    * Writes the source data frame to the given target
    * @param source      the source [[DataFrame]]
    * @param destination the [[Location destination table or location]]
    * @param append      indicates whether the destination should be appended (or conversely overwritten)
    * @param rc          the implicit [[SparkQweryContext]]
    */
  def write(source: DataFrame, destination: Location, append: Boolean)(implicit rc: SparkQweryContext): Unit =
    write(source, destination = rc.getTableOrView(destination), append)

  /**
    * Writes the source data frame to the given target
    * @param source      the source [[DataFrame]]
    * @param destination the [[TableLike destination table or view]]
    * @param append      indicates whether the destination should be appended (or conversely overwritten)
    * @param rc          the implicit [[SparkQweryContext]]
    */
  def write(source: DataFrame, destination: TableLike, append: Boolean)(implicit rc: SparkQweryContext): Unit = destination match {
    case table: Table =>
      val writer = source.write.tableOptions(table).mode(if (append) SaveMode.Append else SaveMode.Overwrite)
      table.outputFormat.orFail("Table output format was not specified") match {
        case AVRO => writer.avro(table.location)
        case CSV => writer.csv(table.location)
        case JDBC => writer.jdbc(table.location, table.name, table.properties || new java.util.Properties())
        case JSON => writer.json(table.location)
        case PARQUET => writer.parquet(table.location)
        case ORC => writer.orc(table.location)
        case format => die(s"Storage format '$format' is not supported for writing")
      }
    case view: View => die(s"View '${view.name}' cannot be modified")
  }

  /**
    * Returns a data frame representing a result set
    * @param name the name of the variable
    */
  case class ReadRowSetByReference(name: String, alias: Option[String]) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = rc.getDataSet(name, alias)
  }

  /**
    * Query table/view reference for Spark
    * @param name the name of the table
    */
  case class ReadTableOrViewByReference(name: String, alias: Option[String]) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = rc.getDataSet(name, alias)
  }

  /**
    * Registers a procedure for use with Spark
    * @param procedure the given [[SparkProcedure]]
    */
  case class RegisterProcedure(procedure: Procedure) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
      logger.info(s"Registering Procedure '${procedure.name}'...")
      rc += SparkProcedure(procedure.name, procedure.params, code = procedure.code.compile)
      None
    }
  }

  /**
    * Registers a table or view for use with Spark
    * @param tableOrView the [[TableLike table or view]]
    */
  case class RegisterTableOrView(tableOrView: TableLike) extends SparkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
      logger.info(s"Registering ${tableOrView.getClass.getSimpleName} '${tableOrView.name}'...")
      val table = tableOrView match {
        case ref@LogicalTable(name, columns, source) => SparkLogicalTable(name, columns, source.compile match {
          case spout: SparkInsert.Spout => spout.copy(resolver = Option(SparkTableColumnResolver(ref)))
          case invokable => invokable
        })
        case other => other
      }
      rc += table
      input
    }
  }

  /**
    * Implicit definitions
    * @author lawrence.daniels@gmail.com
    */
  object Implicits {

    /**
      * Column compiler
      * @param column the given [[Column]]
      */
    final implicit class ColumnCompiler(val column: Column) extends AnyVal {
      @inline def compile: StructField =
        StructField(name = column.name, dataType = toSparkType(column.`type`), nullable = column.nullable)

      @inline def toSparkType(`type`: ColumnType): DataType =
        sparkTypeMapping.getOrElse(`type`, die(s"Type '${`type`}' could not be mapped to Spark"))
    }

    /**
      * Condition compiler
      * @param condition the given [[Condition]]
      */
    final implicit class ConditionCompiler(val condition: Condition) extends AnyVal {
      def compile(implicit rc: SparkQweryContext): SparkColumn = {
        import com.qwery.models.expressions._
        condition match {
          case AND(a, b) => a.compile && b.compile
          case EQ(a, b) => a.compile === b.compile
          case GE(a, b) => a.compile >= b.compile
          case GT(a, b) => a.compile > b.compile
          case IsNotNull(c) => c.compile.isNotNull
          case IsNull(c) => c.compile.isNull
          case LE(a, b) => a.compile <= b.compile
          case LIKE(a, b) => a.compile like b.asString
          case LT(a, b) => a.compile < b.compile
          case NE(a, b) => a.compile =!= b.compile
          case NOT(c) => !c.compile
          case OR(a, b) => a.compile || b.compile
          case RLIKE(a, b) => a.compile rlike b.asString
          case unknown => die(s"Unrecognized condition '$unknown' [${unknown.getClass.getSimpleName}]")
        }
      }
    }

    /**
      * DataFrame Reader Enrichment
      * @param dataFrameReader the given [[DataFrameReader]]
      */
    final implicit class DataFrameReaderEnriched(val dataFrameReader: DataFrameReader) extends AnyVal {
      @inline def tableOptions(tableLike: TableLike): DataFrameReader = {
        var dfr: DataFrameReader = dataFrameReader
        tableLike match {
          case table: Table =>
            table.fieldDelimiter.foreach(delimiter => dfr = dfr.option("delimiter", delimiter))
            table.headersIncluded.foreach(enabled => dfr = dfr.option("header", enabled.toString))
            table.nullValue.foreach(value => dfr = dfr.option("nullValue", value))
          case _ =>
        }
        dfr
      }
    }

    /**
      * DataFrame Writer Enrichment
      * @param dataFrameWriter the given [[DataFrameWriter]]
      */
    final implicit class DataFrameWriterEnriched[T](val dataFrameWriter: DataFrameWriter[T]) extends AnyVal {
      @inline def tableOptions(tableLike: TableLike): DataFrameWriter[T] = {
        var dfw: DataFrameWriter[T] = dataFrameWriter
        tableLike match {
          case table: Table =>
            table.fieldDelimiter.foreach(delimiter => dfw = dfw.option("delimiter", delimiter))
            table.headersIncluded.foreach(enabled => dfw = dfw.option("header", enabled.toString))
            table.nullValue.foreach(value => dfw = dfw.option("nullValue", value))
          case _ =>
        }
        dfw
      }
    }

    /**
      * Expression compiler
      * @param expression the given [[Expression]]
      */
    final implicit class ExpressionCompiler(val expression: Expression) extends AnyVal {
      def compile(implicit rc: SparkQweryContext): SparkColumn = {
        import com.qwery.models.expressions._
        import rc.spark.implicits._
        expression match {
          case Add(a, b) => a.compile + b.compile
          case ref@BasicField(name) => ref.alias.map(alias => /*col(name).as(alias)*/ $"$alias.$name") || $"$name" //col(name)
          case Divide(a, b) => a.compile + b.compile
          case ref@FunctionCall(name, args) =>
            val op = callUDF(name, args.map(_.compile): _*)
            ref.alias.map(alias => op.as(alias)) getOrElse op
          case Literal(value) => lit(value)
          case LocalVariableRef(name) => lit(rc.getVariable(name))
          case Modulo(a, b) => a.compile % b.compile
          case Multiply(a, b) => a.compile * b.compile
          case pow: Pow => die(s"Unsupported feature '**' (power) in $pow")
          case Subtract(a, b) => a.compile - b.compile
          case unknown => die(s"Unrecognized expression '$unknown' [${unknown.getClass.getSimpleName}]")
        }
      }
    }

    /**
      * Invokable compiler
      * @param invokable the given [[Invokable]]
      */
    final implicit class InvokableCompiler(val invokable: Invokable) extends AnyVal {
      def compile(implicit rc: SparkQweryContext): SparkInvokable = invokable match {
        case Assign(variableRef, value) => SparkAssign(variableRef, value = value.compile)
        case Console.Debug(text) => SparkConsole.debug(text)
        case Console.Error(text) => SparkConsole.error(text)
        case Console.Info(text) => SparkConsole.info(text)
        case Console.Log(text) => SparkConsole.log(text)
        case Console.Print(text) => SparkConsole.print(text)
        case Console.Warn(text) => SparkConsole.warn(text)
        case Create(procedure: Procedure) => RegisterProcedure(procedure)
        case Create(tableOrView: TableLike) => RegisterTableOrView(tableOrView)
        case Create(udf: UserDefinedFunction) => SparkRegisterUDF(udf)
        case Include(path) => incorporateSources(path)
        case Insert(destination, source, fields) =>
          SparkInsert(destination = destination.compile, fields = fields, source = source match {
            case Insert.Values(values) =>
              SparkInsert.Spout(
                rows = values.map(_.map(_.asAny)),
                resolver = Option(SparkLocationColumnResolver(destination.target)))
            case op => op.compile
          })
        case Insert.Into(target) => SparkInsert.Sink(target = target, append = true)
        case Insert.Overwrite(target) => SparkInsert.Sink(target = target, append = false)
        case Insert.Values(values) => SparkInsert.Spout(rows = values.map(_.map(_.asAny)), resolver = None)
        case MainProgram(name, code, args, env, hive, streaming) => SparkMain(name, code.compile, args, env, hive, streaming)
        case ProcedureCall(name, args) => SparkProcedureCall(name, args = args.map(_.compile))
        case Return(value) => SparkReturn(value = value.map(_.compile))
        case ref@Select(columns, from, joins, groupBy, orderBy, where, limit) =>
          SparkSelect(columns, from.map(_.compile), joins.map(_.compile), groupBy, orderBy, where, limit, ref.alias)
        case SQL(ops) => SparkSQL(ops.map(_.compile))
        case ref@TableRef(name) => ReadTableOrViewByReference(name, ref.alias)
        case Show(dataSet, limit) => SparkShow(dataSet.compile, limit)
        case Update(table, assignments, where) => die(s"UPDATE is not yet supported")
        case ref@Union(query0, query1) => SparkUnion(query0 = query0.compile, query1 = query1.compile, ref.alias)
        case ref@RowSetVariableRef(name) => ReadRowSetByReference(name, ref.alias)
        case unknown => die(s"Unhandled operation '$unknown'")
      }

      private def incorporateSources(path: String)(implicit rc: SparkQweryContext): SparkInvokable = {
        val sqlLanguageParser = new SQLLanguageParser {}
        val ops = Seq(path) map (new File(_).getCanonicalFile) map { file =>
          logger.info(s"Merging source file '${file.getAbsolutePath}'...")
          sqlLanguageParser.parse(file).compile
        }
        SparkSQL(ops: _*)
      }
    }

    /**
      * Join Enrichment
      * @param join the given [[Join join]]
      */
    final implicit class JoinEnrichment(val join: Join) extends AnyVal {
      @inline def compile(implicit rc: SparkQweryContext): SparkJoin =
        SparkJoin(source = join.source.compile, condition = join.condition.compile, `type` = join.`type`)
    }

    /**
      * Location Enrichment
      * @param location the given [[Location location]]
      */
    final implicit class LocationEnrichment(val location: Location) extends AnyVal {

      /**
        * Attempts to retrieve the desired columns for this [[Location]]
        * @return the collection of [[Column columns]]
        */
      @inline def resolveColumns(implicit rc: SparkQweryContext): List[Column] = rc.getTableOrView(location).resolveColumns

      /**
        * Attempts to retrieve the desired data frame for this [[Location]]
        * @return the [[Table table]]
        */
      @inline def getQuery(implicit rc: SparkQweryContext): Option[DataFrame] = location match {
        case LocationRef(path) => die("Reading from locations is not yet supported")
        case ref@TableRef(name) =>
          val df = read(rc.getTableOrView(name))
          val result = (for {alias <- ref.alias; ndf <- df} yield ndf.as(alias)) ?? df
          df.foreach(_.createOrReplaceTempView(name))
          result
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
      @inline def resolveColumns(implicit rc: SparkQweryContext): List[Column] = {
        tableLike match {
          case table: Table => table.columns
          case table: LogicalTable => table.columns
          case table => die(s"Could not resolve columns for '${table.name}'")
        }
      }
    }

  }

}
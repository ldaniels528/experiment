package com.qwery
package platform
package sparksql.generator

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models._
import com.qwery.models.expressions.SQLFunction._
import com.qwery.models.expressions._
import com.qwery.platform.sparksql.generator.SparkCodeCompiler.Implicits._
import com.qwery.util.OptionHelper._
import com.qwery.util.StringHelper._
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Spark Code Compiler
  * @author lawrence.daniels@gmail.com
  */
trait SparkCodeCompiler {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  def compileCase(model: Case): String = {
    // aggregate the cases into a single operation
    val caseAgg = model.conditions match {
      case first :: remaining =>
        val initialWhen = s"when(${first.condition.compile}, ${first.result.compile})"
        remaining.foldLeft(new StringBuilder(initialWhen)) { case (agg, Case.When(condition, result)) =>
          agg.append(s"\n.when(${condition.compile}, ${result.compile})")
        } toString()
      case _ => die("At least one condition must be specified in CASE statements")
    }

    // optionally, return the case-when with an otherwise clause
    model.otherwise.map(op => caseAgg + s"\n.otherwise(${op.compile})") getOrElse caseAgg
  }

  def discoverTablesAndViews(invokable: Invokable): List[String] = {

    def recurse(invokable: Invokable): List[String] = {
      invokable match {
        case i: Include => recurse(incorporateSources(i.path))
        case j: Join => recurse(j.source)
        case m: MainProgram => recurse(m.code)
        case s: Select => (s.from.toList ::: s.joins.map(_.source).toList).flatMap(recurse)
        case s: SQL => s.statements.flatMap(recurse)
        case u: Union => List(u.query0, u.query1).flatMap(recurse)
        case t: TableLike => t.name :: Nil
        case t: TableRef => t.name :: Nil
        case _ => Nil
      }
    }

    recurse(invokable).distinct
  }

  /**
    * incorporates the source code of the given path
    * @param path the given .sql source file
    * @return the resultant source code
    */
  def incorporateSources(path: String): Invokable = {
    val file = new File(path).getCanonicalFile
    logger.info(s"[*] Merging source file '${file.getAbsolutePath}'...")
    SQLLanguageParser.parse(file)
  }

}

/**
  * Spark Code Compiler
  * @author lawrence.daniels@gmail.com
  */
object SparkCodeCompiler extends SparkCodeCompiler {
  private[this] val resourceMgrClassName = ResourceManager.getObjectSimpleName

  /**
    * Implicit definitions
    */
  object Implicits {

    /**
      * Condition Compiler Extensions
      * @param condition the given [[Condition condition]]
      */
    final implicit class ConditionCompilerExtensions(val condition: Condition) extends AnyVal {
      def compile: String = condition match {
        case AND(a, b) => s"${a.compile} && ${b.compile}"
        case EQ(a, b) => s"${a.compile} === ${b.compile}"
        case GE(a, b) => s"${a.compile} >= ${b.compile}"
        case GT(a, b) => s"${a.compile} > ${b.compile}"
        case IsNotNull(c) => s"${c.compile}.isNotNull"
        case IsNull(c) => s"${c.compile}.isNull"
        case LE(a, b) => s"${a.compile} <= ${b.compile}"
        case LIKE(a, b) => s"${a.compile} like ${b.lit}"
        case LT(a, b) => s"${a.compile} < ${b.compile}"
        case NE(a, b) => s"${a.compile} =!= ${b.compile}"
        case NOT(IsNull(c)) => s"${c.compile}.isNotNull"
        case NOT(c) => s"!${c.compile}"
        case OR(a, b) => s"${a.compile} || ${b.compile}"
        case RLIKE(a, b) => s"${a.compile} rlike ${b.lit}"
        case unknown => die(s"Unrecognized condition '$unknown' [${unknown.getClass.getSimpleName}]")
      }
    }

    /**
      * Expression Compiler Extensions
      * @param expression the given [[Expression expression]]
      */
    final implicit class ExpressionCompilerExtensions(val expression: Expression) extends AnyVal {
      def compile: String = {
        val result = expression match {
          case Abs(a) => s"abs(${a.compile})"
          case Add(a, b) => s"${a.compile} + ${b.compile}"
          case Add_Months(a, b) => s"add_months(${a.compile}, ${b.compile})"
          case AllFields => """col("*")"""
          case Array(args) => s"array(${args.map(_.compile).mkString(",")}: _*)"
          case Array_Contains(a, b) => s"array_contains(${a.compile}, ${b.compile})"
          case _: Array_Index => die("Array index is not supported by Spark")
          case Ascii(a) => s"ascii(${a.compile})"
          case Avg(a) => s"avg(${a.compile})"
          case Base64(a) => s"base64(${a.compile})"
          case BasicField(name) => s"""$$"$name""""
          case Bin(a) => s"bin(${a.compile})"
          case op: Case => compileCase(op)
          case Cast(value, toType) => s"${value.compile}.cast(${toType.compile})"
          case Cbrt(a) => s"cbrt(${a.compile})"
          case Ceil(a) => s"ceil(${a.compile})"
          case Coalesce(args) => s"coalesce(${args.map(_.compile).mkString(",")})"
          case Concat(args) => s"concat(${args.map(_.compile).mkString(",")})"
          case Count(Distinct(a)) => s"countDistinct(${a.compile})"
          case Count(a) => s"count(${a.compile})"
          case Cume_Dist => "cume_dist()"
          case Current_Date => "current_date()"
          case Date_Add(a, b) => s"date_add(${a.compile}, ${b.compile})"
          case Divide(a, b) => s"${a.compile} / ${b.compile}"
          case Factorial(a) => s"factorial(${a.compile})"
          case Floor(a) => s"floor(${a.compile})"
          case From_UnixTime(a, b) => b.map(f => s"from_unixtime(${a.compile}, ${f.compile})") || s"from_unixtime(${a.compile})"
          case FunctionCall(name, args) => s"callUDF(${name.codify}, ${args.map(_.compile).mkString(",")})"
          case If(condition, trueValue, falseValue) => s"when(${condition.compile}, ${trueValue.compile}).otherwise(${falseValue.compile})"
          case JoinField(name, tableAlias) => s"""$$"${tableAlias.map(alias => s"$alias.$name") getOrElse name.codify}""""
          case Length(a) => s"length(${a.compile})"
          case Literal(value) => s"lit(${value.lit})"
          case LocalVariableRef(name) => s"lit(${name.lit})"
          case Lower(a) => s"lower(${a.compile})"
          case LPad(a, b, c) => s"lpad(${a.compile}, ${b.compile}, ${c.compile})"
          case LTrim(a) => s"ltrim(${a.compile})"
          case Max(a) => s"max(${a.compile})"
          case Mean(a) => s"mean(${a.compile})"
          case Min(a) => s"min(${a.compile})"
          case Modulo(a, b) => s"${a.compile} % ${b.compile}"
          case Multiply(a, b) => s"${a.compile} * ${b.compile}"
          case Pow(a, b) => s"pow(${a.compile}, ${b.compile})"
          case RPad(a, b, c) => s"rpad(${a.compile}, ${b.compile}, ${c.compile})"
          case RTrim(a) => s"rtrim(${a.compile})"
          case Split(a, b) => s"split(${a.compile}, ${b.compile})"
          case Subtract(a, b) => s"${a.compile} - ${b.compile}"
          case Substring(a, b, c) => s"substring(${a.compile}, ${b.asInt}, ${c.asInt})"
          case Sum(Distinct(a)) => s"sumDistinct(${a.compile})"
          case Sum(a) => s"sum(${a.compile})"
          case To_Date(a) => s"to_date(${a.compile})"
          case Trim(a) => s"trim(${a.compile})"
          case Upper(a) => s"upper(${a.compile})"
          case Variance(a) => s"variance(${a.compile})"
          case WeekOfYear(a) => s"weekofyear(${a.compile})"
          case Year(a) => s"year(${a.compile})"
          case unknown => die(s"Unrecognized expression '$unknown' [${unknown.getClass.getSimpleName}]")
        }
        result.withAlias(expression)
      }
    }

    /**
      * Invokable Compiler Extensions
      * @param invokable the given [[Invokable]]
      */
    final implicit class InvokableCompilerExtensions(val invokable: Invokable) extends AnyVal {
      def compile(implicit appArgs: ApplicationArgs): String = {
        val result = invokable match {
          case Console.Debug(text) => s"""logger.debug("$text")"""
          case Console.Error(text) => s"""logger.error("$text")"""
          case Console.Info(text) => s"""logger.info("$text")"""
          case Console.Print(text) => s"""println("$text")"""
          case Console.Warn(text) => s"""logger.warn("$text")"""
          case Create(t: Table) => s"""$resourceMgrClassName.add(${t.codify})"""
          case Include(path) => incorporateSources(path).compile
          case i: Insert => i.compile
          case m: MainProgram => m.code.compile
          case s: Select => s.compile
          case Show(rows, limit) => s"""${rows.compile}.show(${limit.getOrElse(20)})"""
          case s: SQL => s.statements.map(_.compile).mkString("\n")
          case t: TableRef => t.name
          case x =>
            throw new IllegalArgumentException(s"Unsupported operation ${Option(x).map(_.getClass.getName).orNull}")
        }
        result.withAlias(invokable)
      }
    }

    /**
      * Insert Compiler Extensions
      * @param insert the given [[Insert]]
      */
    final implicit class InsertCompilerExtensions(val insert: Insert) extends AnyVal {
      import com.qwery.util.StringHelper._

      @inline def compile(implicit appArgs: ApplicationArgs): String =
        s"""|${ResourceManager.getObjectSimpleName}.write(
            |   source = ${insert.source.compile},
            |   destination = TableManager("${insert.destination.target.compile}"),
            |   append = ${insert.destination.isInstanceOf[Insert.Into]}
            |)""".stripMargin
    }

    /**
      * Map Compiler Extensions
      * @param mapping the given [[Map mapping]]
      */
    final implicit class MapCodifyExtension(val mapping: Map[String, String]) extends AnyVal {
      @inline def codify: String = mapping map { case (k, v) => k.codify -> v.codify } mkString ","
    }

    /**
      * Select Compiler Extensions
      * @param select the given [[Select]]
      */
    final implicit class SelectCompilerExtensions(val select: Select) extends AnyVal {

      def compile(implicit appArgs: ApplicationArgs): String = if (appArgs.isInlineSQL) compileAsSQL else compileAsCode

      private def compileAsCode(implicit appArgs: ApplicationArgs): String = {
        val source = select.from map {
          case TableRef(name) => s"$resourceMgrClassName.read(${name.codify})"
          case op => op.compile
        } getOrElse die(s"The data source found for '$select'")

        // process the SELECT statement
        pipeline.foldLeft(new StringBuilder(source)) { (sb, fx) =>
          fx(select).foreach(s => sb.append("\n").append(s))
          sb
        } toString()
      }

      private def compileAsSQL(implicit appArgs: ApplicationArgs): String = {
        import SparkInlineSQLCompiler.Implicits._

        val quote = "\"\"\""
        val first = s"$quote|"
        val last = s"\t||$quote.stripMargin('|')"
        val list = Source.fromString(select.toSQL).getLines().toList
        val lines = first :: list.map(line => s"\t||$line") ::: last :: Nil
        s"spark.sql(\n${lines mkString "\n"})"
      }

      private def pipeline(implicit appArgs: ApplicationArgs): Seq[Select => Option[String]] = Seq(
        processWhere, processJoin, processGroupBy, processFields, processOrderBy, processLimit
      )

      private def processFields(select: Select): Option[String] = {
        if (select.fields.isEmpty) None else {
          Option(s".select(${
            select.fields.map {
              case field: Field => if (field.isAggregate) s"""$$"${field.getName}"""" else field.compile
              case fx: SQLFunction => if (fx.isAggregate) s"""$$"${fx.getName}"""" else fx.compile
              case expr => expr.compile
            } mkString ","
          })")
        }
      }

      private def processGroupBy(select: Select): Option[String] = {
        if (select.groupBy.isEmpty) None else Option(
          s"""|.groupBy(${select.groupBy.map(_.compile).mkString(",")})
              |.agg(${
            select.fields.collect {
              case field: Field if field.isAggregate => field.compile
              case fx: SQLFunction if fx.isAggregate => fx.compile
            } mkString ",\n"
          })""".stripMargin)
      }

      private def processJoin(select: Select)(implicit appArgs: ApplicationArgs): Option[String] = {
        if (select.joins.isEmpty) None else Option {
          select.joins.map { join =>
            import join._
            s""".join(${source.compile}, ${condition.compile}, "${`type`.toString.toLowerCase()}")"""
          } mkString "\n"
        }
      }

      private def processLimit(select: Select): Option[String] = select.limit.map(n => s".limit($n)")

      private def processOrderBy(select: Select): Option[String] =
        if (select.orderBy.nonEmpty) Option(s".orderBy(${select.orderBy.map(col => s"$$$col").mkString(",")})") else None

      private def processWhere(select: Select): Option[String] = select.where.map(cond => s".where(${cond.compile})")

    }

    /**
      * String Compiler Extensions
      * @param values the given [[String value]]
      */
    final implicit class StringSeqCompilerExtensions(val values: Seq[String]) extends AnyVal {
      @inline def compile: String = values.map(s => s""""$s"""").mkString(",")
    }

    /**
      * Storage Format Compiler Extensions
      * @param storageFormat the given [[StorageFormat]]
      */
    final implicit class StorageFormatExtensions(val storageFormat: StorageFormat) extends AnyVal {
      @inline def compile: String = s"StorageFormats.$storageFormat"
    }

    /**
      * String Compiler Extensions
      * @param string the given [[String value]]
      */
    final implicit class StringCompilerExtensions(val string: String) extends AnyVal {

      @inline def codify: String = s""""$string""""

      @inline def withAlias(aliasable: Aliasable): String =
        aliasable.alias.map(alias => s"""$string.as(${alias.lit})""") getOrElse string

      @inline def withAlias(invokable: Invokable): String = invokable match {
        case a: Aliasable => a.alias.map(alias => s"""$string.as(${alias.lit})""") getOrElse string
        case _ => string
      }
    }

    /**
      * Table Column Compiler Extensions
      * @param column the given [[Column]]
      */
    final implicit class TableColumnExtensions(val column: Column) extends AnyVal {
      import column._

      @inline def codify: String = s"""Column(name = "$name", `type` = ${`type`.compile}, isNullable = $isNullable)"""
    }

    /**
      * Table Column Type Compiler Extensions
      * @param columnType the given [[ColumnType]]
      */
    final implicit class TableColumnTypeExtensions(val columnType: ColumnType) extends AnyVal {
      @inline def compile: String = s"ColumnTypes.$columnType"
    }

    /**
      * Table Compiler Extensions
      * @param tableLike the given [[TableLike]]
      */
    final implicit class TableExtensions(val tableLike: TableLike) extends AnyVal {
      def codify: String = tableLike match {
        case table: Table =>
          import table._
          s"""|Table(
              |  name = "$name",
              |  columns = List(${columns.map(_.codify).mkString(",")}),
              |  location = "$location",
              |  fieldDelimiter = ${fieldDelimiter.map(_.codify)},
              |  fieldTerminator = ${fieldTerminator.map(_.codify)},
              |  headersIncluded = $headersIncluded,
              |  nullValue = ${nullValue.map(_.codify)},
              |  inputFormat = ${inputFormat.map(_.compile)},
              |  outputFormat = ${outputFormat.map(_.compile)},
              |  partitionColumns = List(${partitionColumns.map(_.codify).mkString(",")}),
              |  properties = Map(${properties.codify}),
              |  serdeProperties = Map(${serdeProperties.codify})
              |)""".stripMargin
        case table => die(s"Table type '${table.getClass.getSimpleName}' is not yet supported")
      }
    }

    /**
      * Value Compiler Extensions
      * @param value the given value
      */
    final implicit class ValueCompilerExtensions(val value: Any) extends AnyVal {

      @inline def asInt: Int = value.lit.toDouble.toInt

      @inline def lit: String = value match {
        case null => "NULL"
        case d: Double => if (d == d.toLong) d.toLong.toString else d.toString
        case Literal(_value) => _value.lit
        case s: String => s""""$s""""
        case x => x.toString
      }
    }

  }

}

package com.qwery
package platform
package sparksql.generator

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models._
import com.qwery.models.expressions.SQLFunction._
import com.qwery.models.expressions._
import com.qwery.platform.sparksql.generator.SparkCodeCompiler.Implicits._
import com.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Spark Code Compiler
  * @author lawrence.daniels@gmail.com
  */
trait SparkCodeCompiler {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Generates a complete list of defined tables and views
    * @param invokable the top-level [[Invokable invokable]]
    * @return a list of defined [[TableLike tables and views]]
    */
  def discoverTablesAndViews(invokable: Invokable): List[TableLike] = {

    /**
      * Recusively traverses the given object graph
      * @param invokable the given [[Invokable object graph]]
      * @return the distinct collection of [[TableLike tables and views]]
      */
    def recurse(invokable: Invokable): List[TableLike] = invokable match {
      case Create(table: Table) => table :: Nil
      case Create(view: View) => view :: Nil
      case i: Include => recurse(incorporateSources(i.path))
      case m: MainProgram => recurse(m.code)
      case s: SQL => s.statements.flatMap(recurse)
      case _ => Nil
    }

    recurse(invokable).distinct
  }

  /**
    * Incorporates the source code of the given path
    * @param path the given .sql source file
    * @return the resultant source code
    */
  def incorporateSources(path: String): Invokable = {
    val file = new File(path).getCanonicalFile
    logger.info(s"[*] Merging source file '${file.getAbsolutePath}'...")
    SQLLanguageParser.parse(file)
  }

  def makeSQL(op: Case): String = {
    val sb = new StringBuilder("CASE")
    op.conditions foreach { case Case.When(condition, result) =>
      sb.append(s" WHEN ${condition.toSQL} THEN ${result.toSQL}\n")
    }
    op.otherwise.foreach(expr => sb.append(s" ELSE ${expr.toSQL}"))
    sb.append(" END")
    sb.toString()
  }

  def makeSQL(op: Select)(implicit settings: ApplicationSettings): String = {
    val sb = new StringBuilder()
    sb.append("SELECT\n")
    sb.append(op.fields.map(_.toSQL).mkString(",\n"))
    op.from foreach { from =>
      val result = from match {
        case a: Aliasable if a.alias.nonEmpty => s"(\n ${a.toSQL} \n)"
        case x => x.toSQL
      }
      sb.append(s"\nFROM ${result.withAlias(from)}")
    }
    if (op.joins.nonEmpty) sb.append(s"\n${op.joins.map(_.toSQL).mkString("\n")}")
    op.where foreach { condition => sb.append(s"\nWHERE ${condition.toSQL}") }
    if (op.groupBy.nonEmpty) sb.append(s"\nGROUP BY ${op.groupBy.map(_.toSQL).mkString(",")}")
    op.having foreach { condition => sb.append(s"\nHAVING ${condition.toSQL}") }
    if (op.orderBy.nonEmpty) sb.append(s"\nORDER BY ${op.orderBy.map(_.toSQL).mkString(",")}")
    sb.toString()
  }

}

/**
  * Spark Code Compiler
  * @author lawrence.daniels@gmail.com
  */
object SparkCodeCompiler extends SparkCodeCompiler {

  /**
    * Implicit definitions
    */
  object Implicits {

    final implicit class ConditionSQLCompiler(val condition: Condition) extends AnyVal {
      def toSQL: String = condition match {
        case AND(a, b) => s"${a.toSQL} AND ${b.toSQL}"
        case EQ(a, b) => s"${a.toSQL} = ${b.toSQL}"
        case GE(a, b) => s"${a.toSQL} >= ${b.toSQL}"
        case GT(a, b) => s"${a.toSQL} > ${b.toSQL}"
        case IsNotNull(c) => s"${c.toSQL} IS NOT NULL"
        case IsNull(c) => s"${c.toSQL} IS NULL"
        case LE(a, b) => s"${a.toSQL} <= ${b.toSQL}"
        case LIKE(a, b) => s"${a.toSQL} like ${b.asLit}"
        case LT(a, b) => s"${a.toSQL} < ${b.toSQL}"
        case NE(a, b) => s"${a.toSQL} <> ${b.toSQL}"
        case NOT(IsNull(c)) => s"${c.toSQL}.isNotNull"
        case NOT(c) => s"NOT ${c.toSQL}"
        case OR(a, b) => s"${a.toSQL} OR ${b.toSQL}"
        case RLIKE(a, b) => s"${a.toSQL} RLIKE ${b.asLit}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }
    }

    final implicit class ExpressionSQLCompiler(val expression: Expression) extends AnyVal {
      def toSQL: String = {
        val result = expression match {
          case Abs(a) => s"ABS(${a.toSQL})"
          case Add(a, b) => s"${a.toSQL} + ${b.toSQL}"
          case Add_Months(a, b) => s"ADD_MONTHS(${a.toSQL}, ${b.toSQL})"
          case AllFields => "*"
          case Array(args) => s"ARRAY(${args.map(_.toSQL).mkString(",")})"
          case Array_Contains(a, b) => s"ARRAY_CONTAINS(${a.toSQL}, ${b.toSQL})"
          case Array_Distinct(args) => s"ARRAY_DISTINCT(${args.map(_.toSQL).mkString(",")})"
          case Array_Except(a, b) => s"ARRAY_EXCEPT(${a.toSQL}, ${b.toSQL})"
          case Array_Intersect(a, b) => s"ARRAY_INTERSECT(${a.toSQL}, ${b.toSQL})"
          case Array_Max(a) => s"ARRAY_MAX(${a.toSQL})"
          case Array_Min(a) => s"ARRAY_MIN(${a.toSQL})"
          case Array_Position(a, b) => s"Array_Position(${a.toSQL}, ${b.toSQL})"
          case Ascii(a) => s"ASCII(${a.toSQL})"
          case Avg(a) => s"AVG(${a.toSQL})"
          case Base64(a) => s"BASE64(${a.toSQL})"
          case BasicField(name) => name
          case Bin(a) => s"BIN(${a.toSQL})"
          case BitwiseAND(a, b) => s"${a.toSQL} & ${b.toSQL}"
          case BitwiseOR(a, b) => s"${a.toSQL} | ${b.toSQL}"
          case BitwiseXOR(a, b) => s"${a.toSQL} ^ ${b.toSQL}"
          case c: Case => makeSQL(c)
          case Cast(value, toType) => s"CAST(${value.toSQL} AS ${toType.toSQL})"
          case Cbrt(a) => s"CBRT(${a.toSQL})"
          case Ceil(a) => s"CEIL(${a.toSQL})"
          case Coalesce(args) => s"COALESCE(${args.map(_.toSQL).mkString(",")})"
          case Concat(args) => s"CONCAT(${args.map(_.toSQL).mkString(",")})"
          case Count(Distinct(a)) => s"COUNT(DISTINCT(${a.toSQL}))"
          case Count(a) => s"COUNT(${a.toSQL})"
          case Cume_Dist => "CUME_DIST()"
          case Current_Database => "Current_Database()"
          case Current_Date => "CURRENT_DATE()"
          case Date_Add(a, b) => s"DATE_ADD(${a.toSQL}, ${b.toSQL})"
          case Divide(a, b) => s"${a.toSQL} / ${b.toSQL}"
          case Factorial(a) => s"FACTORIAL(${a.toSQL})"
          case Floor(a) => s"FLOOR(${a.toSQL})"
          case From_UnixTime(a, b) => s"FROM_UNIXTIME(${a.toSQL}, ${b.toSQL})"
          case FunctionCall(name, args) => s"CALLUDF(${name.asLit}, ${args.map(_.toSQL).mkString(",")})"
          case If(condition, trueValue, falseValue) => s"IF(${condition.toSQL}, ${trueValue.toSQL}, ${falseValue.toSQL})"
          case JoinField(name, tableAlias) => tableAlias.map(alias => s"$alias.$name") getOrElse name
          case Length(a) => s"LENGTH(${a.toSQL})"
          case Literal(value) => value.asLit
          case LocalVariableRef(name) => name.asLit
          case Lower(a) => s"LOWER(${a.toSQL})"
          case LPad(a, b, c) => s"LPAD(${a.toSQL}, ${b.toSQL}, ${c.toSQL})"
          case LTrim(a) => s"LTRIM(${a.toSQL})"
          case Max(a) => s"MAX(${a.toSQL})"
          case Mean(a) => s"MEAN(${a.toSQL})"
          case Min(a) => s"MIN(${a.toSQL})"
          case Modulo(a, b) => s"${a.toSQL} % ${b.toSQL}"
          case Multiply(a, b) => s"${a.toSQL} * ${b.toSQL}"
          case Pow(a, b) => s"POW(${a.toSQL}, ${b.toSQL})"
          case RPad(a, b, c) => s"RPAD(${a.toSQL}, ${b.toSQL}, ${c.toSQL})"
          case RTrim(a) => s"RTRIM(${a.toSQL})"
          case Split(a, b) => s"SPLIT(${a.toSQL}, ${b.toSQL})"
          case Subtract(a, b) => s"${a.toSQL} - ${b.toSQL}"
          case Substring(a, b, c) => s"SUBSTRING(${a.toSQL}, ${b.asInt}, ${c.asInt})"
          case Sum(Distinct(a)) => s"SUM(DISTINCT(${a.toSQL}))"
          case Sum(a) => s"SUM(${a.toSQL})"
          case To_Date(a) => s"TO_DATE(${a.toSQL})"
          case Trim(a) => s"TRIM(${a.toSQL})"
          case Upper(a) => s"UPPER(${a.toSQL})"
          case Variance(a) => s"VARIANCE(${a.toSQL})"
          case WeekOfYear(a) => s"WEEKOFYEAR(${a.toSQL})"
          case Year(a) => s"YEAR(${a.toSQL})"
          case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
        }
        result.withAlias(expression)
      }
    }

    /**
      * Invokable Compiler Extensions
      * @param invokable the given [[Invokable]]
      */
    final implicit class InvokableCompilerExtensions(val invokable: Invokable) extends AnyVal {
      def compile(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        val result = invokable match {
          case Console.Debug(text) => s"""logger.debug("$text")"""
          case Console.Error(text) => s"""logger.error("$text")"""
          case Console.Info(text) => s"""logger.info("$text")"""
          case Console.Print(text) => s"""println("$text")"""
          case Console.Warn(text) => s"""logger.warn("$text")"""
          case Create(tableOrView: TableLike) => sparkRead(tableOrView)
          case Include(path) => incorporateSources(path).compile
          case i: Insert => i.compile
          case m: MainProgram => m.code.compile
          case s: Select => s.compile
          case Show(rows, limit) => s"""${rows.compile}.show(${limit.getOrElse(20)})"""
          case s: SQL => s.statements.map(_.compile).mkString("\n")
          case t: TableRef => t.name
          case x => throw new IllegalStateException(s"Unsupported operation ${Option(x).map(_.getClass.getName).orNull}")
        }
        result
      }

      private def sparkRead(tableLike: TableLike): String = {
        tableLike match {
          case table: Table =>
            table.inputFormat.map(_.toString.toLowerCase()) match {
              case Some(format) =>
                s"""|spark.read.$format("${table.location}")
                    |   .${defineColumns(table)}
                    |   .createOrReplaceGlobalTempView("${table.name}")
                    |""".stripMargin
              case None => ""
            }
          case other => die(s"Table entity '${other.name}' could not be translated")
        }
      }

      private def defineColumns(table: Table): String = s"""toDF(${table.columns.map(_.name.codify).mkString(",")})"""
    }

    final implicit class InvokableSQLCompiler(val invokable: Invokable) extends AnyVal {
      def toSQL(implicit settings: ApplicationSettings): String = {
        val result = invokable match {
          case s: Select => makeSQL(s)
          case s: SQL => s.statements.map(_.toSQL).mkString("\n")
          case t: TableRef =>
            val tableName = s"${settings.defaultDB}.${t.name}"
            t.alias.map(alias => s"$tableName AS $alias") getOrElse tableName
          case u: Union => s"${u.query0.toSQL} UNION ${if (u.isDistinct) "DISTINCT" else ""} ${u.query1.toSQL}"
          case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
        }
        result //.withAlias(invokable)
      }
    }

    final implicit class JoinSQLCompiler(val join: Join) extends AnyVal {
      def toSQL(implicit settings: ApplicationSettings): String = {
        s"${join.`type`.toString.replaceAllLiterally("_", " ")} JOIN ${
          val result = join.source match {
            case a: Aliasable if a.alias.nonEmpty => s"(\n ${a.toSQL} \n)"
            case x => x.toSQL
          }
          result.withAlias(join.source)
        } ON ${join.condition.toSQL}"
      }
    }

    final implicit class OrderColumnSQLCompiler(val orderColumn: OrderColumn) extends AnyVal {
      def toSQL: String = orderColumn match {
        case o: OrderColumn => s"${o.name} ${if (o.isAscending) "ASC" else "DESC"}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }
    }

    /**
      * Table Column Type Compiler Extensions
      * @param columnType the given [[ColumnType]]
      */
    final implicit class TableColumnTypeExtensions(val columnType: ColumnType) extends AnyVal {
      @inline def toSQL: String = columnType.toString.replaceAllLiterally("_", "")
    }

    /**
      * Insert Compiler Extensions
      * @param insert the given [[Insert]]
      */
    final implicit class InsertCompilerExtensions(val insert: Insert) extends AnyVal {
      @inline def compile(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        ctx.lookupTableOrView(insert.destination.target.compile) match {
          case table: InlineTable => die(s"Inline table '${table.name}' is read-only")
          case table: Table =>
            // determine the output type (e.g. "CSV" -> "csv") and mode (append or overwrite)
            val writer = table.outputFormat.orFail("Table output format was not specified").toString.toLowerCase()

            // build the expression
            val buf = ListBuffer[String]()
            buf += s"${insert.source.compile}.write"
            buf ++= table.fieldDelimiter.map(delimiter => s""".option("delimiter", "$delimiter")""").toList
            buf ++= table.headersIncluded.map(enabled => s""".option("header", "$enabled")""").toList
            buf ++= table.nullValue.map(value => s""".option("nullValue", "$value")""").toList
            buf ++= (table.properties ++ table.serdeProperties).map { case (key, value) => s""".option("$key", "$value")""" }
            buf += s""".mode(${if (insert.destination.isAppend) "SaveMode.Append" else "SaveMode.Overwrite"})"""
            buf += s""".$writer("${table.location}")\n"""
            buf.mkString("\n")
          case view: View => die(s"View '${view.name}' is read-only")
        }
      }
    }

    /**
      * Select Compiler Extensions
      * @param select the given [[Select]]
      */
    final implicit class SelectCompilerExtensions(val select: Select) extends AnyVal {
      def compile(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        val quote = "\"\"\""
        val buf = ListBuffer[String]()
        buf += s"$quote|"
        buf ++= Source.fromString(select.toSQL).getLines().map(line => s"|$line")
        buf += s"|$quote.stripMargin('|')"
        s"spark.sql(\n${buf mkString "\n"})\n"
      }
    }

    /**
      * String SQLCompiler Extensions
      * @param string the given [[String value]]
      */
    final implicit class StringCompilerExtensions(val string: String) extends AnyVal {
      @inline def codify: String = s""""$string""""

      @inline def withAlias(aliasable: Aliasable): String =
        aliasable.alias.map(alias => s"$string AS $alias") getOrElse string

      @inline def withAlias(invokable: Invokable): String = invokable match {
        case a: Aliasable => a.alias.map(alias => s"$string AS $alias") getOrElse string
        case _ => string
      }
    }

    /**
      * String Compiler Extensions
      * @param values the given [[String value]]
      */
    final implicit class StringSeqCompilerExtensions(val values: Seq[String]) extends AnyVal {
      @inline def compile: String = values.map(s => '"' + s + '"').mkString(",")
    }

    /**
      * Value Compiler Extensions
      * @param value the given value
      */
    final implicit class ValueCompilerExtensionA(val value: Any) extends AnyVal {

      @inline def asInt: Int = value.asLit.toDouble.toInt

      @inline def asLit: String = value match {
        case null => "NULL"
        case Literal(_value) => _value.asLit
        case s: String => s"'$s'"
        case x => x.toString
      }
    }

  }

}

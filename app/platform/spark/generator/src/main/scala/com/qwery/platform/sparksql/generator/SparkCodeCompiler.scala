package com.qwery
package platform
package sparksql.generator

import java.io.File

import com.qwery.language.SQLLanguageParser
import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.Insert.Values
import com.qwery.models._
import com.qwery.models.expressions._
import com.qwery.platform.sparksql.generator.SparkCodeCompiler.Implicits._
import com.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Spark SQL/Code Compiler
  * @author lawrence.daniels@gmail.com
  */
trait SparkCodeCompiler {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * Recusively traverses the given object graph
    * @param invokable the given [[Invokable object graph]]
    * @return the distinct collection of [[TableLike tables and views]]
    */
  def find[T](invokable: Invokable)(f: Invokable => List[T]): List[T] = invokable match {
    case i: Include => find(incorporateSources(i.path))(f)
    case s: SQL => s.statements.flatMap(find(_)(f))
    case x => f(x)
  }

  /**
    * Generates a complete list of defined procedures
    * @param root the top-level [[Invokable invokable]]
    * @return a list of defined [[Procedure procedures]]
    */
  def findProcedures(root: Invokable): List[Procedure] = find[Procedure](root) {
    case Create(procedure: Procedure) => procedure :: Nil
    case _ => Nil
  }

  /**
    * Generates a complete list of defined tables and views
    * @param root the top-level [[Invokable invokable]]
    * @return a list of defined [[TableLike tables and views]]
    */
  def findTablesAndViews(root: Invokable): List[TableLike] = find[TableLike](root) {
    case Create(table: TableLike) => table :: Nil
    case _ => Nil
  }

  /**
    * Generates the a `toDF` Scala expression
    * @param columns the given collection of [[Column columns]]
    * @return the Scala Code string (e.g. `toDF("name", "price", "location")`)
    */
  def generateCode(columns: List[Column]): String = s"toDF(${columns.map(_.name.toCode).mkString(",")})"

  /**
    * Generates a Spark `write` expression
    * @param insert the given [[Insert insert]]
    * @return the Scala Code string
    */
  def generateCode(insert: Insert)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    ctx.lookupTableOrView(insert.destination.target.toCode) match {
      case table: InlineTable => die(s"Inline table '${table.name}' is read-only")
      case table: Table =>
        // determine the output type (e.g. "CSV" -> "csv") and mode (append or overwrite)
        val writer = table.outputFormat.orFail("Table output format was not specified").toString.toLowerCase()

        // build the expression
        val buf = ListBuffer[String]()
        buf += s"${insert.source.toCode}${generateOptions(table)}"
        buf += ".write"
        buf ++= (table.properties ++ table.serdeProperties).map { case (key, value) => s""".option("$key", "$value")""" }
        buf += s""".mode(${if (insert.destination.isAppend) "SaveMode.Append" else "SaveMode.Overwrite"})"""
        buf += s""".$writer("${table.location}")\n"""
        buf.mkString("\n")
      case view: View => die(s"View '${view.name}' is read-only")
    }
  }

  /**
    * Generates a Procedure expression
    * @param model the given [[Procedure model]]
    * @return the Scala Code string
    */
  def generateCode(model: Procedure)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    import model._
    new StringBuilder()
      .append(s"""def $name(${params.map(_.toCode).mkString(",")}) = {""")
      .append(s"\n  ${code.toCode}")
      .append("\n}\n")
      .toString()
  }

  /**
    * Generates the SQL CREATE TEMPORARY FUNCTION statement
    * @param udf the given [[UserDefinedFunction model]]
    * @return the Scala Code string
    */
  def generateCode(udf: UserDefinedFunction): String =
    s"""registerUDF(name = "${udf.name}", `class` = "${udf.`class`}")\n"""

  /**
    * Generates the SQL VALUES ( ... ) statement
    * @param model the given [[Values model]]
    * @return the Scala Code string
    */
  def generateCode(model: Values): String = {
    s"""Seq(${
      model.values.filterNot(_.isEmpty) map {
        case List(expression) => expression.asCode
        case list => '(' + list.map(_.asCode).mkString(",") + ')'
      } mkString ","
    })"""
  }

  def generateCode(model: While)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    s"""|while(${model.condition.toCode}) {
        |  ${model.invokable.toCode}
        |}
        |""".stripMargin
  }

  def generateOptions(table: Table): String = {
    new StringBuilder()
      .append(table.fieldDelimiter.map(delimiter => s"""\n  .option("delimiter", "$delimiter")""").getOrElse(""))
      .append(table.headersIncluded.map(enabled => s"""\n   .option("header", "$enabled")""").getOrElse(""))
      .append(table.nullValue.map(value => s"""\n   .option("nullValue", "$value")""").getOrElse(""))
      .toString()
  }

  def generateReader(tableLike: TableLike)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    tableLike match {
      case InlineTable(name, columns, source) =>
        s"""|${source.toCode}
            |   .${generateCode(columns)}
            |   .withGlobalTempView("$name")""".stripMargin
      case table: Table =>
        table.inputFormat.map(_.toString.toLowerCase()) match {
          case Some(format) =>
            s"""|spark.read${generateOptions(table)}
                |   .$format("${table.location}")
                |   .${generateCode(table.columns)}
                |   .withGlobalTempView("${table.name}")""".stripMargin
          case None => ""
        }
      case View(name, query) =>
        s"""|${query.toCode}
            |   .withGlobalTempView("$name")""".stripMargin
      case other => die(s"Table entity '${other.name}' could not be translated")
    }
  }

  /**
    * Generates the SQL Case statements
    * @param model the given [[Case model]]
    * @return the SQL string
    */
  def generateSQL(model: Case): String = {
    val sb = new StringBuilder("CASE")
    model.conditions foreach { case Case.When(condition, result) =>
      sb.append(s" WHEN ${condition.toSQL} THEN ${result.toSQL}\n")
    }
    model.otherwise.foreach(expr => sb.append(s" ELSE ${expr.toSQL}"))
    sb.append(" END")
    sb.toString()
  }

  /**
    * Generates the SQL Select statement
    * @param model the given [[Select model]]
    * @return the SQL string
    */
  def generateSQL(model: Select)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    val sb = new StringBuilder()
    sb.append("SELECT\n")
    sb.append(model.fields.map(_.toSQL).mkString(",\n"))
    model.from foreach { from =>
      val result = from match {
        case a: Aliasable if a.alias.nonEmpty => s"(\n ${a.toSQL} \n)"
        case x => x.toSQL
      }
      sb.append(s"\nFROM ${result.withAlias(from)}")
    }
    if (model.joins.nonEmpty) sb.append(s"\n${model.joins.map(_.toSQL).mkString("\n")}")
    model.where foreach { condition => sb.append(s"\nWHERE ${condition.toSQL}") }
    if (model.groupBy.nonEmpty) sb.append(s"\nGROUP BY ${model.groupBy.map(_.toSQL).mkString(",")}")
    model.having foreach { condition => sb.append(s"\nHAVING ${condition.toSQL}") }
    if (model.orderBy.nonEmpty) sb.append(s"\nORDER BY ${model.orderBy.map(_.toSQL).mkString(",")}")
    sb.toString()
  }

  def generateSQL(model: While)(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
    s"""|WHILE ${model.condition.toSQL}
        |BEGIN
        |  ${model.invokable.toSQL}
        |END
        |""".stripMargin
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

    /**
      * Column Compiler Extensions
      * @param column the given [[Column column]]
      */
    final implicit class ColumnEnrichment(val column: Column) extends AnyVal {
      def toCode: String = s"${column.name}:${column.`type`.toCode}"
    }

    /**
      * Condition Compiler Extensions
      * @param condition the given [[Condition condition]]
      */
    final implicit class ConditionCompiler(val condition: Condition) extends AnyVal {

      def toCode: String = condition match {
        case AND(a, b) => s"${a.toCode} && ${b.toCode}"
        case EQ(a, b) => s"${a.toCode} == ${b.toCode}"
        case GE(a, b) => s"${a.toCode} >= ${b.toCode}"
        case GT(a, b) => s"${a.toCode} > ${b.toCode}"
        case IsNotNull(c) => s"${c.toCode} != null"
        case IsNull(c) => s"${c.toCode} == null"
        case LE(a, b) => s"${a.toCode} <= ${b.toCode}"
        case l: LocalVariableRef => l.name
        case LT(a, b) => s"${a.toCode} < ${b.toCode}"
        case NE(a, b) => s"${a.toCode} <> ${b.toCode}"
        case NOT(IsNull(c)) => s"${c.toCode} != null"
        case NOT(c) => s"!${c.toCode}"
        case OR(a, b) => s"${a.toCode} || ${b.toCode}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into Scala")
      }

      def toSQL: String = condition match {
        case AND(a, b) => s"${a.toSQL} AND ${b.toSQL}"
        case EQ(a, b) => s"${a.toSQL} = ${b.toSQL}"
        case GE(a, b) => s"${a.toSQL} >= ${b.toSQL}"
        case GT(a, b) => s"${a.toSQL} > ${b.toSQL}"
        case IsNotNull(c) => s"${c.toSQL} IS NOT NULL"
        case IsNull(c) => s"${c.toSQL} IS NULL"
        case LE(a, b) => s"${a.toSQL} <= ${b.toSQL}"
        case LIKE(a, b) => s"${a.toSQL} like ${b.asSQL}"
        case l: LocalVariableRef => s"""s'$$${l.name}'"""
        case LT(a, b) => s"${a.toSQL} < ${b.toSQL}"
        case NE(a, b) => s"${a.toSQL} <> ${b.toSQL}"
        case NOT(IsNull(c)) => s"${c.toSQL}.isNotNull"
        case NOT(c) => s"NOT ${c.toSQL}"
        case OR(a, b) => s"${a.toSQL} OR ${b.toSQL}"
        case RLIKE(a, b) => s"${a.toSQL} RLIKE ${b.asSQL}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }
    }

    /**
      * Expression Compiler Extensions
      * @param expression the given [[Expression expression]]
      */
    final implicit class ExpressionCompiler(val expression: Expression) extends AnyVal {

      def toCode: String = expression match {
        case FunctionCall(name, args) => s"$name(${args.map(_.toCode).mkString(",")})"
        case Literal(value) => value.asCode
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }

      def toSQL: String = {
        val result = expression match {
          case Add(a, b) => s"${a.toSQL} + ${b.toSQL}"
          case AllFields => "*"
          case BasicField(name) => name
          case BitwiseAND(a, b) => s"${a.toSQL} & ${b.toSQL}"
          case BitwiseOR(a, b) => s"${a.toSQL} | ${b.toSQL}"
          case BitwiseXOR(a, b) => s"${a.toSQL} ^ ${b.toSQL}"
          case c: Case => generateSQL(c)
          case Cast(value, toType) => s"CAST(${value.toSQL} AS ${toType.toSQL})"
          case FunctionCall(name, args) => s"$name(${args.map(_.toSQL).mkString(",")})"
          case If(condition, trueValue, falseValue) => s"IF(${condition.toSQL}, ${trueValue.toSQL}, ${falseValue.toSQL})"
          case Literal(value) => value.asSQL
          case LocalVariableRef(name) => name.asSQL
          case Modulo(a, b) => s"${a.toSQL} % ${b.toSQL}"
          case Multiply(a, b) => s"${a.toSQL} * ${b.toSQL}"
          case Pow(a, b) => s"POW(${a.toSQL}, ${b.toSQL})"
          case Subtract(a, b) => s"${a.toSQL} - ${b.toSQL}"
          case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
        }
        result.withAlias(expression)
      }
    }

    /**
      * Insert Compiler Extensions
      * @param insert the given [[Insert]]
      */
    final implicit class InsertCompilerExtensions(val insert: Insert) extends AnyVal {
      @inline def toCode(implicit settings: ApplicationSettings, ctx: CompileContext): String = generateCode(insert)
    }

    /**
      * Invokable Compiler Extensions
      * @param invokable the given [[Invokable]]
      */
    final implicit class InvokableCompilerExtensions(val invokable: Invokable) extends AnyVal {

      @inline def procedures: Seq[Procedure] = findProcedures(invokable)

      @inline def tablesAndViews: Seq[TableLike] = findTablesAndViews(invokable)

      def toCode(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        val result = invokable match {
          case Console(name, text) if name == "print" => s"""println("$text")"""
          case Console(name, text) if name == "log" => s"""logger.info("$text")"""
          case Console(name, text) => s"""logger.$name("$text")"""
          case Create(procedure: Procedure) => generateCode(procedure)
          case Create(tableOrView: TableLike) => generateReader(tableOrView)
          case Create(udf: UserDefinedFunction) => generateCode(udf)
          case FileSystem(path) => s"""getFiles("$path")"""
          case Include(path) => incorporateSources(path).toCode
          case i: Insert => i.toCode
          case l: LocalVariableRef => l.name
          case ProcedureCall(name, args) => s"""$name(${args.map(_.toCode).mkString(",")})"""
          case s: Select => s.toCode
          case SetRowVariable(name, dataSet) => s"""val $name = ${dataSet.toCode}"""
          case RowSetVariableRef(name) => name
          case SetLocalVariable(name, expression) => s"""val $name = ${expression.toCode}"""
          case Show(rows, limit) => s"${rows.toCode}.show(${limit.getOrElse(20)})"
          case s: SQL => s.statements.map(_.toCode).mkString("\n")
          case t: TableRef => t.name
          case v: Values => generateCode(v)
          case w: While => generateCode(w)
          case z => throw new IllegalStateException(s"Unsupported operation $z")
        }
        result
      }

      def toSQL(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        val result = invokable match {
          case s: Select => generateSQL(s)
          case s: SQL => s.statements.map(_.toSQL).mkString("\n")
          case t: TableRef =>
            val tableName = s"${settings.defaultDB}.${t.name}"
            t.alias.map(alias => s"$tableName AS $alias") getOrElse tableName
          case u: Union => s"${u.query0.toSQL} UNION ${if (u.isDistinct) "DISTINCT" else ""} ${u.query1.toSQL}"
          case w: While => generateSQL(w)
          case z => die(s"Model class '${Option(z).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
        }
        result //.withAlias(invokable)
      }
    }

    /**
      * Join Compiler Extension
      * @param join the given [[Join model]]
      */
    final implicit class JoinCompilerExtension(val join: Join) extends AnyVal {
      @inline def toSQL(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        s"${join.`type`.toString.replaceAllLiterally("_", " ")} JOIN ${
          val result = join.source match {
            case a: Aliasable if a.alias.nonEmpty => s"(\n ${a.toSQL} \n)"
            case x => x.toSQL
          }
          result.withAlias(join.source)
        } ON ${join.condition.toSQL}"
      }
    }

    /**
      * Order Column Compiler Extension
      * @param orderColumn the given [[OrderColumn model]]
      */
    final implicit class OrderColumnCompiler(val orderColumn: OrderColumn) extends AnyVal {
      def toSQL: String = orderColumn match {
        case o: OrderColumn => s"${o.name} ${if (o.isAscending) "ASC" else "DESC"}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }
    }

    /**
      * Select Compiler Extensions
      * @param select the given [[Select]]
      */
    final implicit class SelectCompilerExtensions(val select: Select) extends AnyVal {
      def toCode(implicit settings: ApplicationSettings, ctx: CompileContext): String = {
        Source.fromString(select.toSQL).getLines().toList match {
          case first :: remaining =>
            val quote = "\"\"\""
            val buf = ListBuffer[String]()
            buf += s"s$quote|$first"
            buf ++= remaining.map(line => s"|$line")
            buf += s"|$quote.stripMargin('|')"
            s"spark.sql(\n${buf mkString "\n"})\n"
          case _ => die(s"Corrupted SELECT statement [$select]")
        }
      }
    }

    /**
      * String SQLCompiler Extensions
      * @param string the given [[String value]]
      */
    final implicit class StringCompilerExtensions(val string: String) extends AnyVal {
      @inline def toCode: String = string.asCode

      @inline def withAlias(model: Aliasable): String =
        model.alias.map(alias => s"$string AS $alias") getOrElse string

      @inline def withAlias(model: Invokable): String = model match {
        case a: Aliasable => a.alias.map(alias => s"$string AS $alias") getOrElse string
        case _ => string
      }
    }

    /**
      * String Compiler Extensions
      * @param values the given [[String value]]
      */
    final implicit class StringSeqCompilerExtensions(val values: Seq[String]) extends AnyVal {
      @inline def toCode: String = values.map(s => '"' + s + '"').mkString(",")
    }

    /**
      * Table Column Type Compiler Extensions
      * @param columnType the given [[ColumnType]]
      */
    final implicit class TableColumnTypeExtensions(val columnType: ColumnType) extends AnyVal {
      @inline def toCode: String = columnType.toString.replaceAllLiterally("_", "").toLowerCase.capitalize

      @inline def toSQL: String = toCode
    }

    /**
      * Value Compiler Extensions
      * @param value the given value
      */
    final implicit class ValueCompilerExtensionA(val value: Any) extends AnyVal {

      @inline def asCode: String = value match {
        case s: String => s""""$s""""
        case Literal(_value) => _value.asCode
        case x => x.toString
      }

      @inline def asInt: Int = asDouble.toInt

      @inline def asDouble: Double = value.asSQL.toDouble

      @inline def asSQL: String = value match {
        case null => "NULL"
        case Literal(_value) => _value.asSQL
        case s: String => s"'$s'"
        case x => x.toString
      }
    }

  }

}

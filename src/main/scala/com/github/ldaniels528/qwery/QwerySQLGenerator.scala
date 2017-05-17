package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.NamedExpression.{AggregateAlias, ExpressionAlias}
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.builtins._
import com.github.ldaniels528.qwery.ops.math._
import com.github.ldaniels528.qwery.ops.types._
import com.github.ldaniels528.qwery.sources._
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Qwery SQL Generator
  * @author lawrence.daniels@gmail.com
  */
object QwerySQLGenerator {

  private def makeSQL(condition: Condition): String = condition match {
    case AND(a, b) => s"${a.toSQL} AND ${b.toSQL}"
    case EQ(a, b) => s"${a.toSQL} = ${b.toSQL}"
    case GE(a, b) => s"${a.toSQL} >= ${b.toSQL}"
    case GT(a, b) => s"${a.toSQL} > ${b.toSQL}"
    case LE(a, b) => s"${a.toSQL} <= ${b.toSQL}"
    case LIKE(a, b) => s"${a.toSQL} LIKE ${b.toSQL}"
    case LT(a, b) => s"${a.toSQL} < ${b.toSQL}"
    case NE(a, b) => s"${a.toSQL} <> ${b.toSQL}"
    case NOT(cond) => s"NOT ${cond.toSQL}"
    case OR(a, b) => s"${a.toSQL} OR ${b.toSQL}"
    case unknown =>
      throw new IllegalArgumentException(s"Condition '$unknown' was unhandled")
  }

  private def makeSQL(executable: Executable): String = executable match {
    case Describe(source, limit) => toDescribe(source, limit)
    case Insert(target, fields, source, append, hints) => toInsert(target, fields, source, append, hints)
    case InsertValues(_, dataSets) =>
      dataSets.map(dataSet => s"VALUES (${dataSet.map(_.toSQL).mkString(", ")})").mkString(" ")
    case Select(fields, source, condition, groupFields, orderedColumns, limit) =>
      toSelect(fields, source, condition, groupFields, orderedColumns, limit)
    case unknown =>
      throw new IllegalArgumentException(s"Executable '$unknown' was unhandled")
  }

  private def makeSQL(expression: Expression): String = expression match {
    case Add(a, b) => s"${a.toSQL} + ${b.toSQL}"
    case AggregateAlias(name, expr) => s"${expr.toSQL} AS ${nameOf(name)}"
    case Avg(expr) => s"AVG(${expr.toSQL})"
    case BasicField(name) => nameOf(name)
    case BooleanValue(value) => value.toString
    case Case(conditions, otherwise) => toCase(conditions, otherwise)
    case Cast(expr, toType) => s"CAST(${expr.toSQL} AS $toType)"
    case Count(expr) => s"COUNT(${expr.toSQL})"
    case Concat(a, b) => s"${a.toSQL} || ${b.toSQL}"
    case Divide(a, b) => s"${a.toSQL} / ${b.toSQL}"
    case ExpressionAlias(name, expr) => s"${expr.toSQL} AS ${nameOf(name)}"
    case FunctionRef(name, args) => s"$name(${args.map(_.toSQL).mkString(", ")})"
    case Left(a, b) => s"LEFT(${a.toSQL}, ${b.toSQL})"
    case Len(expr) => s"LEN(${expr.toSQL})"
    case Max(expr) => s"MAX(${expr.toSQL})"
    case Min(expr) => s"MIN(${expr.toSQL})"
    case Multiply(a, b) => s"${a.toSQL} * ${b.toSQL}"
    case Now => "NOW()"
    case NumericValue(value) => value.toString
    case Right(a, b) => s"RIGHT(${a.toSQL}, ${b.toSQL})"
    case Split(a, b) => s"SPLIT(${a.toSQL},${b.toSQL})"
    case Sqrt(expr) => s"SQRT(${expr.toSQL})"
    case StringValue(value) => s"'$value'"
    case Subtract(a, b) => s"${a.toSQL} - ${b.toSQL}"
    case Substring(a, b, c) => s"SUBSTRING(${a.toSQL},${b.toSQL},${c.toSQL})"
    case Sum(expr) => s"SUM(${expr.toSQL})"
    case Trim(expr) => s"TRIM(${expr.toSQL})"
    case Uuid => "UUID()"
    case unknown =>
      throw new IllegalArgumentException(s"Expression '$unknown' was unhandled")
  }

  private def makeSQL(value: AnyRef): String = value match {
    case Hints(delimiter, headers, quoted) => s"HINTS(DELIMITER '$delimiter', HEADERS ${headers.onOff}, QUOTES ${quoted.onOff})"
    case OrderedColumn(name, ascending) => s"${nameOf(name)} ${if (ascending) "ASC" else "DESC"}"
    case QueryResource(path) => s"'$path'"
    case condition: Condition => makeSQL(condition)
    case executable: Executable => makeSQL(executable)
    case expression: Expression => makeSQL(expression)
    case output: QueryOutputSource => makeSQL(output)
    case unknown => s"'$unknown'"
  }

  private def nameOf(name: String): String = if (name.forall(_.isLetterOrDigit)) name else s"`$name`"

  private def toCase(conditions: Seq[Case.When], otherwise: Option[Expression]): String = {
    val sb = new StringBuilder("CASE")
    conditions foreach { case Case.When(condition, result) =>
      sb.append(s" WHEN ${condition.toSQL} THEN ${result.toSQL}")
    }
    otherwise.foreach(expr => sb.append(s" ELSE ${expr.toSQL}"))
    sb.append(" END")
    sb.toString()
  }

  private def toDescribe(source: QueryResource, limit: Option[Int]): String = {
    val sb = new StringBuilder(s"DESCRIBE ${source.toSQL}")
    limit.foreach(n => sb.append(s" LIMIT $n"))
    sb.toString()
  }

  private def toInsert(target: QueryResource, fields: Seq[Field], source: Executable, append: Boolean, hints: Hints): String = {
    s"""
       |INSERT ${if (append) "INTO" else "OVERWRITE"} ${target.toSQL} (${fields.map(_.toSQL).mkString(", ")})
       |${source.toSQL}""".stripMargin.toSingleLine
  }

  private def toSelect(fields: Seq[Expression],
                       source: Option[QueryResource],
                       condition: Option[Condition],
                       groupFields: Seq[Field],
                       orderedColumns: Seq[OrderedColumn],
                       limit: Option[Int]): String = {
    val sb = new StringBuilder(s"SELECT ${fields.map(_.toSQL) mkString ", "}")
    source.foreach(src => sb.append(s" FROM ${src.toSQL}"))
    condition.foreach(where => sb.append(s" WHERE ${where.toSQL}"))
    if (groupFields.nonEmpty) sb.append(s" GROUP BY ${groupFields.map(_.toSQL) mkString ", "}")
    if (orderedColumns.nonEmpty) sb.append(s" ORDER BY ${orderedColumns.map(_.toSQL) mkString ", "}")
    limit.foreach(n => sb.append(s" LIMIT $n"))
    sb.toString
  }

  final implicit class SQLExtensions(val value: AnyRef) extends AnyVal {
    def toSQL: String = makeSQL(value)
  }

  final implicit class ConditionExtensions(val condition: Condition) extends AnyVal {
    def toSQL: String = makeSQL(condition)
  }

  final implicit class ExecutableExtensions(val executable: Executable) extends AnyVal {
    def toSQL: String = makeSQL(executable)
  }

  final implicit class ExpressionExtensions(val expression: Expression) extends AnyVal {
    def toSQL: String = makeSQL(expression)
  }

}

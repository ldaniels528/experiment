package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.NamedExpression.{NamedAggregation, NamedAlias}
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.builtins._
import com.github.ldaniels528.qwery.sources._

/**
  * Qwery SQL Conversion
  * @author lawrence.daniels@gmail.com
  */
object QwerySQLConversion {

  def makeSQL(value: AnyRef): String = value match {
    case hints: Hints => toHint(hints)
    case OrderedColumn(name, ascending) => s"${nameOf(name)} ${if (ascending) "ASC" else "DESC"}"
    case condition: Condition => makeSQL(condition)
    case executable: Executable => makeSQL(executable)
    case expression: Expression => makeSQL(expression)
    case output: OutputSource => makeSQL(output)
    case unknown => s"'$unknown'"
  }

  private def makeSQL(condition: Condition): String = condition match {
    case AND(a, b) => s"${a.toSQL} AND ${b.toSQL}"
    case EQ(a, b) => s"${a.toSQL} = ${b.toSQL}"
    case GE(a, b) => s"${a.toSQL} >= ${b.toSQL}"
    case GT(a, b) => s"${a.toSQL} > ${b.toSQL}"
    case LE(a, b) => s"${a.toSQL} <= ${b.toSQL}"
    case LIKE(a, b) => s"${a.toSQL} LIKE ${b.toSQL}"
    case LT(a, b) => s"${a.toSQL} < ${b.toSQL}"
    case MATCHES(a, b) => s"${a.toSQL} MATCHES ${b.toSQL}"
    case NE(a, b) => s"${a.toSQL} <> ${b.toSQL}"
    case NOT(cond) => s"NOT ${cond.toSQL}"
    case OR(a, b) => s"${a.toSQL} OR ${b.toSQL}"
    case unknown =>
      throw new IllegalArgumentException(s"Condition '$unknown' was unhandled")
  }

  private def makeSQL(executable: Executable): String = executable match {
    case Assignment(variableRef, expression) => s"SET $variableRef = ${expression.toSQL}"
    case CodeBlock(operations) => s"BEGIN ${operations.map(_.toSQL).mkString("; ")} END"
    case DataResource(path, hints) => toDataResource(path, hints)
    case Declare(variableRef, typeName) => s"DECLARE $variableRef $typeName"
    case Describe(source, limit) => toDescribe(source, limit)
    case Insert(target, fields, source) => toInsert(target, fields, source)
    case InsertValues(_, dataSets) =>
      dataSets.map(dataSet => s"VALUES (${dataSet.map(_.toSQL).mkString(", ")})").mkString(" ")
    case Procedure(name, params, operation) =>
      s"CREATE PROCEDURE $name(${params.map(_.toSQL).mkString(",")}) AS ${operation.toSQL}"
    case Select(fields, source, condition, groupFields, orderedColumns, limit) =>
      toSelect(fields, source, condition, groupFields, orderedColumns, limit)
    case Union(a, b) => toUnion(a, b)
    case View(name, query) => s"CREATE VIEW $name AS ${query.toSQL}"
    case unknown =>
      throw new IllegalArgumentException(s"Executable '$unknown' was unhandled")
  }

  private def makeSQL(expression: Expression): String = expression match {
    case Add(a, b) => s"${a.toSQL} + ${b.toSQL}"
    case AllFields => "*"
    case Avg(expr) => s"AVG(${expr.toSQL})"
    case BasicField(name) => nameOf(name)
    case Case(conditions, otherwise) => toCase(conditions, otherwise)
    case Cast(expr, toType) => s"CAST(${expr.toSQL} AS $toType)"
    case Concat(a, b) => s"${a.toSQL} || ${b.toSQL}"
    case Count(expr) => s"COUNT(${expr.toSQL})"
    case ConstantValue(value) => toConstantValue(value)
    case Divide(a, b) => s"${a.toSQL} / ${b.toSQL}"
    case FunctionRef(name, args) => s"$name(${args.map(_.toSQL).mkString(", ")})"
    case Left(a, b) => s"LEFT(${a.toSQL}, ${b.toSQL})"
    case Len(expr) => s"LEN(${expr.toSQL})"
    case Max(expr) => s"MAX(${expr.toSQL})"
    case Min(expr) => s"MIN(${expr.toSQL})"
    case Multiply(a, b) => s"${a.toSQL} * ${b.toSQL}"
    case NamedAggregation(name, expr) => s"${expr.toSQL} AS ${nameOf(name)}"
    case NamedAlias(name, expr) => s"${expr.toSQL} AS ${nameOf(name)}"
    case Now => "NOW()"
    case Pow(a, b) => s"${a.toSQL} ** ${b.toSQL}"
    case Right(a, b) => s"RIGHT(${a.toSQL}, ${b.toSQL})"
    case Split(a, b) => s"SPLIT(${a.toSQL},${b.toSQL})"
    case Sqrt(expr) => s"SQRT(${expr.toSQL})"
    case Substring(a, b, c) => s"SUBSTRING(${a.toSQL},${b.toSQL},${c.toSQL})"
    case Subtract(a, b) => s"${a.toSQL} - ${b.toSQL}"
    case Sum(expr) => s"SUM(${expr.toSQL})"
    case Trim(expr) => s"TRIM(${expr.toSQL})"
    case Uuid => "UUID()"
    case unknown =>
      throw new IllegalArgumentException(s"Expression '$unknown' was unhandled")
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

  private def toConstantValue(value: Any) = value.asInstanceOf[AnyRef] match {
    case n: Number => n.toString
    case s => s"'$s'"
  }

  private def toDataResource(path: String, hints: Option[Hints]) = {
    val sb = new StringBuilder(80)
    sb.append(s"'$path'")
    hints.foreach(hints => sb.append(hints.toSQL))
    sb.toString()
  }

  private def toDescribe(source: Executable, limit: Option[Int]) = {
    val sb = new StringBuilder(s"DESCRIBE ")
    source match {
      case ds: DataResource => sb.append(ds.toSQL)
      case exec: Executable => sb.append(s"(${exec.toSQL})")
    }
    limit.foreach(n => sb.append(s" LIMIT $n"))
    sb.toString()
  }

  private def toDisconnect(handle: String) = s"DISCONNECT FROM '$handle'"

  private def toHint(hints: Hints) = {
    val sb = new StringBuilder(80)
    hints.delimiter.foreach(delimiter => sb.append(s" WITH DELIMITER '$delimiter'"))
    hints.gzip.foreach(on => if (on) sb.append(" WITH GZIP COMPRESSION"))
    hints.headers.foreach(on => if (on) sb.append(" WITH COLUMN HEADERS"))
    hints.isJson.foreach(on => if (on) sb.append(" WITH JSON FORMAT"))
    hints.quotedNumbers.foreach(on => if (on) sb.append(" WITH QUOTED NUMBERS"))
    hints.quotedText.foreach(on => if (on) sb.append(" WITH QUOTED TEXT"))
    sb.toString()
  }

  private def toInsert(target: DataResource, fields: Seq[Field], source: Executable): String = {
    s"""INSERT ${
      if (target.hints.exists(_.isAppend)) "INTO" else "OVERWRITE"
    } ${target.toSQL} (${
      fields.map(_.toSQL).mkString(", ")
    }) ${source.toSQL}"""
  }

  private def toSelect(fields: Seq[Expression],
                       source: Option[Executable],
                       condition: Option[Condition],
                       groupFields: Seq[Field],
                       orderedColumns: Seq[OrderedColumn],
                       limit: Option[Int]): String = {
    val sb = new StringBuilder(s"SELECT ${fields.map(_.toSQL) mkString ", "}")
    source match {
      case Some(ds: DataResource) => sb.append(s" FROM ${ds.toSQL}")
      case Some(exec) => sb.append(s" FROM (${exec.toSQL})")
      case None =>
    }
    condition.foreach(where => sb.append(s" WHERE ${where.toSQL}"))
    if (groupFields.nonEmpty) sb.append(s" GROUP BY ${groupFields.map(_.toSQL) mkString ", "}")
    if (orderedColumns.nonEmpty) sb.append(s" ORDER BY ${orderedColumns.map(_.toSQL) mkString ", "}")
    limit.foreach(n => sb.append(s" LIMIT $n"))
    sb.toString
  }

  private def toUnion(a: Executable, b: Executable) = s"${a.toSQL} UNION ${b.toSQL}"

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

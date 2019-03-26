package com.qwery.platform.codegen.sparksql

import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models._
import com.qwery.models.expressions.SQLFunction._
import com.qwery.models.expressions._
import com.qwery.util.OptionHelper._

/**
  * Spark Inline SQL Compiler
  * @author lawrence.daniels@gmail.com
  */
object SparkInlineSQLCompiler {

  def makeSQL(op: Case): String = {
    val sb = new StringBuilder("CASE")
    op.conditions foreach { case Case.When(condition, result) =>
      sb.append(s" WHEN ${condition.toSQL} THEN ${result.toSQL}\n")
    }
    op.otherwise.foreach(expr => sb.append(s" ELSE ${expr.toSQL}"))
    sb.append(" END")
    sb.toString()
  }

  def makeSQL(op: Select)(implicit settings: CompilerSettings): String = {
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
        case Array(args) => s"array(${args.map(_.toSQL).mkString(",")}: _*)"
        case Array_Contains(a, b) => s"array_contains(${a.toSQL}, ${b.toSQL})"
        case _: Array_Index => die("Array index is not supported by Spark")
        case Ascii(a) => s"ASCII(${a.toSQL})"
        case Avg(a) => s"AVG(${a.toSQL})"
        case Base64(a) => s"BASE64(${a.toSQL})"
        case BasicField(name) => name
        case Bin(a) => s"BIN(${a.toSQL})"
        case c: Case => makeSQL(c)
        case Cast(value, toType) => s"CAST(${value.toSQL} AS ${toType.toSQL})"
        case Cbrt(a) => s"cbrt(${a.toSQL})"
        case Ceil(a) => s"ceil(${a.toSQL})"
        case Coalesce(args) => s"COALESCE(${args.map(_.toSQL).mkString(",")})"
        case Concat(args) => s"CONCAT(${args.map(_.toSQL).mkString(",")})"
        case Count(Distinct(a)) => s"COUNT(DISTINCT(${a.toSQL}))"
        case Count(a) => s"COUNT(${a.toSQL})"
        case Cume_Dist => "CUME_DIST()"
        case Current_Date => "CURRENT_DATE()"
        case Date_Add(a, b) => s"date_add(${a.toSQL}, ${b.toSQL})"
        case Divide(a, b) => s"${a.toSQL} / ${b.toSQL}"
        case Factorial(a) => s"FACTORIAL(${a.toSQL})"
        case Floor(a) => s"FLOOR(${a.toSQL})"
        case From_UnixTime(a, b) => b.map(f => s"FROM_UNIXTIME(${a.toSQL}, ${f.toSQL})") || s"FROM_UNIXTIME(${a.toSQL})"
        case FunctionCall(name, args) => s"callUDF(${name.asLit}, ${args.map(_.toSQL).mkString(",")})"
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
        case Substring(a, b, c) => s"SUBSTR(${a.toSQL}, ${b.asInt}, ${c.asInt})"
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

  final implicit class InvokableSQLCompiler(val invokable: Invokable) extends AnyVal {
    def toSQL(implicit settings: CompilerSettings): String = {
      val result = invokable match {
        case s: Select => makeSQL(s)
        case s: SQL => s.statements.map(_.toSQL).mkString("\n")
        case t: TableRef =>
          val tableName = s"${settings.getDefaultDB}.${t.name}"
          t.alias.map(alias => s"$tableName AS $alias") getOrElse tableName
        case u: Union => s"${u.query0.toSQL} UNION ${if (u.isDistinct) "DISTINCT" else ""} ${u.query1.toSQL}"
        case x => die(s"Model class '${Option(x).map(_.getClass.getSimpleName).orNull}' could not be translated into SQL")
      }
      result //.withAlias(invokable)
    }
  }

  final implicit class JoinSQLCompiler(val join: Join) extends AnyVal {
    def toSQL(implicit settings: CompilerSettings): String = {
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
    * String SQLCompiler Extensions
    * @param string the given [[String value]]
    */
  final implicit class StringSQLCompilerExtensions(val string: String) extends AnyVal {

    @inline def withAlias(aliasable: Aliasable): String =
      aliasable.alias.map(alias => s"$string AS $alias") getOrElse string

    @inline def withAlias(invokable: Invokable): String = invokable match {
      case a: Aliasable => a.alias.map(alias => s"$string AS $alias") getOrElse string
      case _ => string
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

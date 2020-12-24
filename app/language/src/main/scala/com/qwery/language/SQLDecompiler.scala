package com.qwery
package language

import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models._
import com.qwery.models.expressions._

/**
  * SQL Decompiler
  */
object SQLDecompiler {

  def decompile(expression: Expression): String = expression match {
    case f@BasicField(name) => name.withAlias(f.alias)
    case f@FunctionCall(name, args) => decompileFunctionCall(name, args).withAlias(f.alias)
    case l@Literal(value) => value.toString.withAlias(l.alias)
    case other => die(s"Failed to decompile - $other")
  }

  def decompile(invokable: Invokable): String = invokable match {
    case Create(View(name, query, ifNotExists)) => decompileCreateView(name, query, ifNotExists)
    case select: Select => decompileSelect(select).withAlias(select.alias)
    case t@TableRef(name) => name.withAlias(t.alias)
    case other => die(s"Failed to decompile - $other")
  }

  private def decompileCreateView(name: String, query: Invokable, ifNotExists: Boolean): String = {
    s"create view $name ${if (ifNotExists) "if not exists" else ""} as ${query.toSQL}".replaceAllLiterally("  ", " ")
  }

  private def decompileFunctionCall(name: String, args: List[Expression]): String = {
    s"$name(${args.map(_.toSQL).mkString(",")})"
  }

  private def decompileSelect(select: Select): String = {
    import select._
    s"""|select ${fields.map(_.toSQL).mkString(",")}
        |${from.map(ref => s"from ${ref.toSQL}").orBlank}
        |${where.map(cond => s"where ${cond.toSQL}").orBlank}
        |${if (groupBy.nonEmpty) s"group by ${groupBy.map(_.toSQL).mkString(",")}" else ""}
        |${if (orderBy.nonEmpty) s"order by ${orderBy.map(_.toSQL).mkString(",")}" else ""}
        |${limit.map(n => s"limit $n").orBlank}
        |""".stripMargin.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")
  }

  /**
    * SQL deserialization implicits
    */
  object implicits {

    final implicit class AliasHelper(val sql: String) extends AnyVal {
      def withAlias(alias_? : Option[String]): String = alias_?.map(alias => s"$sql as $alias").getOrElse(sql)
    }

    final implicit class ExpressionDeserializer(val expression: Expression) extends AnyVal {
      def toSQL: String = decompile(expression)
    }

    final implicit class InvokableDeserializer(val invokable: Invokable) extends AnyVal {
      def toSQL: String = decompile(invokable)
    }

    final implicit class OptionHelper(val option: Option[String]) extends AnyVal {
      @inline def orBlank: String = option.getOrElse("")
    }

    final implicit class OrderColumnDeserializer(val oc: OrderColumn) extends AnyVal {
      def toSQL: String = s"${oc.name} ${if (oc.isAscending) "asc" else "desc"}"
    }

  }

}

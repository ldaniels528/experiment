package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.util.StringHelper._

import scala.language.postfixOps

/**
  * Represents a describe statement
  * @author lawrence.daniels@gmail.com
  */
case class Describe(source: Executable, limit: Option[Int] = None) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val rows = source.execute(scope).take(1)
    val header = if (rows.hasNext) rows.next().columns else Nil
    ResultSet(rows = header.take(limit getOrElse Int.MaxValue) map { case (name, value) =>
      Seq(
        "Column" -> name,
        "Type" -> Option(value).map(_.getClass.getSimpleName).orNull,
        "Sample" -> Option(value).map(_.toString.toSingleLine).orNull) : Row
    } toIterator)
  }

}

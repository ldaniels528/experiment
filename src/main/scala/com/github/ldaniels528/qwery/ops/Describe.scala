package com.github.ldaniels528.qwery.ops

import scala.language.postfixOps

/**
  * Represents a describe statement
  * @author lawrence.daniels@gmail.com
  */
case class Describe(source: Executable, limit: Option[Int] = None) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val rows = source.execute(scope).take(1)
    val header = if (rows.hasNext) rows.next() else Map.empty
    header.take(limit getOrElse Int.MaxValue).toSeq map { case (name, value) =>
      Seq("COLUMN" -> name, "TYPE" -> value.getClass.getSimpleName, "SAMPLE" -> value)
    } toIterator
  }

}

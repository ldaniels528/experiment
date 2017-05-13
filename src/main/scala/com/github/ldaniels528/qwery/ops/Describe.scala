package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ResultSet
import com.github.ldaniels528.qwery.sources.QueryInputSource

/**
  * Represents a describe statement
  * @author lawrence.daniels@gmail.com
  */
case class Describe(source: QueryInputSource, limit: Option[Int]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val rows = source.execute(scope).toIterator.take(5)
    val header = if (rows.hasNext) rows.next() else Map.empty
    val results = header.take(limit getOrElse Int.MaxValue).toSeq map { case (name, value) =>
      Seq("COLUMN" -> name, "TYPE" -> value.getClass.getSimpleName, "SAMPLE" -> value)
    }
    results
  }

  override def toSQL: String = {
    val sb = new StringBuilder(s"DESCRIBE $source")
    limit.foreach(n => sb.append(s" LIMIT $n"))
    sb.toString()
  }

}

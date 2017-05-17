package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.QueryResource

/**
  * Represents a describe statement
  * @author lawrence.daniels@gmail.com
  */
case class Describe(source: QueryResource, limit: Option[Int]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val device = source.getInputSource.getOrElse(throw new IllegalStateException(s"No such device '${source.path}'"))
    val rows = device.execute(scope).toIterator.take(5)
    val header = if (rows.hasNext) rows.next() else Map.empty
    header.take(limit getOrElse Int.MaxValue).toSeq map { case (name, value) =>
      Seq("COLUMN" -> name, "TYPE" -> value.getClass.getSimpleName, "SAMPLE" -> value)
    }
  }

}

package com.github.ldaniels528.qwery.ops

import java.util.concurrent.atomic.AtomicInteger

import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.sources.QueryInputSource

import scala.language.postfixOps

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(source: Option[QueryInputSource],
                  fields: Seq[Evaluatable],
                  condition: Option[Expression] = None,
                  groupFields: Option[Seq[Field]] = None,
                  sortFields: Option[Seq[(Field, Int)]] = None,
                  limit: Option[Int] = None)
  extends Executable {

  override def execute(scope: Scope): ResultSet = source match {
    case Some(device) =>
      val rows = device.execute(this)
        .map { r => scope.update(r); r }
        .filter(r => condition.isEmpty || condition.exists(_.satisfies(scope)))
        .toIterator
        .take(limit getOrElse Int.MaxValue)

      if (fields.isAllFields) rows.map(_.toSeq) else rows.map(filterRow)
    case None =>
      Iterator.empty
  }

  private def filterRow(row: Map[String, Any]): Row = {
    val counter = new AtomicInteger()
    fields.flatMap {
      case field: Field => row.get(field.name).map(v => field.name -> v)
      case NumericValue(value) => Some(s"$$${counter.addAndGet(1)}" -> value)
      case StringValue(value) => Some(s"$$${counter.addAndGet(1)}" -> value)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value type '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

}

package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.sources.QueryInputSource

import scala.collection.TraversableOnce

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Selection(source: Option[QueryInputSource],
                     fields: Seq[Field],
                     condition: Option[Expression],
                     limit: Option[Int])
  extends Query {

  override def execute(): TraversableOnce[Seq[(String, Any)]] = source match {
    case Some(device) =>
      val rows = device.execute(this)
      filterFields(fields, rows)
    case None => Iterator.empty
  }

  private def filterFields(fields: Seq[Field], rows: TraversableOnce[Map[String, Any]]) = {
    rows map { row =>
      fields.flatMap(field => row.get(field.name).map(v => field.name -> v))
    }
  }

}

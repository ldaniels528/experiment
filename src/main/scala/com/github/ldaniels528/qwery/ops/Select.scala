package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.QueryInputSource
import com.github.ldaniels528.qwery.{Expression, Field, ResultSet, Row}

import scala.collection.TraversableOnce
import scala.language.postfixOps

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(source: Option[QueryInputSource],
                  fields: Seq[Field],
                  condition: Option[Expression],
                  limit: Option[Int])
  extends Query {

  override def execute(): ResultSet = source match {
    case Some(device) => filterFields(fields, device.execute(this))
    case None => Iterator.empty
  }

  private def filterFields(fields: Seq[Field], rows: TraversableOnce[Map[String, Any]]): ResultSet = {
    val allFields = fields.exists(_.name == "*")
    if (allFields)
      rows.map(row => row.keys.toSet ++ fields.map(_.name) flatMap (name => row.get(name).map(value => name -> value)) toSeq)
    else
      rows.map(filterRow(_, fields))
  }

  private def filterRow(row: Map[String, Any], fields: Seq[Field]): Row = {
    fields.flatMap(field => row.get(field.name).map(v => field.name -> v))
  }

}

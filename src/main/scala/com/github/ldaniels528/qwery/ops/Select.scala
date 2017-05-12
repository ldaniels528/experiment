package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops.Select._
import com.github.ldaniels528.qwery.sources.QueryInputSource

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(source: Option[QueryInputSource],
                  fields: Seq[Expression],
                  condition: Option[Condition] = None,
                  groupFields: Option[Seq[Field]] = None,
                  sortFields: Option[Seq[(Field, Int)]] = None,
                  limit: Option[Int] = None)
  extends Executable {

  override def execute(scope: Scope): ResultSet = source match {
    case Some(device) =>
      val rows = device.execute(this)
        .map(row => LocalScope(scope, row))
        .filter(rowScope => condition.isEmpty || condition.exists(_.isSatisfied(rowScope)))
        .toIterator
        .take(limit getOrElse Int.MaxValue)

      // is this an aggregate query?
      if (isAggregate) {
        // collect the aggregates
        val aggregates = fields.collect {
          case fx: AggregateFunction => fx
        }

        // update each aggregate field, and return the evaluated results
        rows.foreach(rowScope => aggregates.foreach(_.update(rowScope)))
        Seq(aggregates.flatMap(field => field.evaluate(scope).map(scope.getName(field) -> _)))
      }

      // otherwise, it's a normal query
      else {
        if (fields.isAllFields) rows.map(_.data) else rows.map(filterRow)
      }
    case None =>
      Seq(fields.flatMap(field => field.evaluate(scope).map(scope.getName(field) -> _)))
  }

  /**
    * Indicates whether this is an aggregate query
    * @return true, if at least one field is an aggregate-only field
    */
  def isAggregate: Boolean = fields.exists {
    case _: AggregateFunction => true
    case _ => false
  }

  private def filterRow(scope: LocalScope): Row = {
    fields.flatMap(ev => ev.evaluate(scope).map(value => scope.getName(ev) -> value))
  }

}

/**
  * Select Companion
  * @author lawrence.daniels@gmail.com
  */
object Select {

  /**
    * Value Sequence Extensions
    * @param values the given collection of values
    */
  implicit class ValueSeqExtensions(val values: Seq[Expression]) extends AnyVal {

    @inline
    def isAllFields: Boolean = values.exists {
      case field: Field => field.name == "*"
      case _ => false
    }
  }

}
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
      val rows = device.execute(scope)
        .toIterator
        .map(row => LocalScope(scope, row))
        .filter(rowScope => condition.isEmpty || condition.exists(_.isSatisfied(rowScope)))
        .take(limit getOrElse Int.MaxValue)

      // is this an aggregate query?
      if (isAggregate) {
        // collect the aggregates
        val groupFieldNames = groupFields.getOrElse(Nil).map(_.name)
        val aggregates = fields.collect {
          case field: Field if groupFieldNames.contains(field.name) => new AggregateField(field.name)
          case fx: AggregateFunction => fx
        }

        // update each aggregate field, and return the evaluated results
        groupFields match {
          case Some(_) => groupBy(scope, rows, aggregates, groupFieldNames)
          case None =>
            rows.foreach(rowScope => aggregates.foreach(_.update(rowScope)))
            Seq(aggregates.map(expand(scope, _)))
        }
      }

      // otherwise, it's a normal query
      else {
        if (fields.isAllFields) rows.map(_.data) else rows.map(filterRow)
      }
    case None =>
      Seq(fields.map(expand(scope, _)))
  }

  def groupBy(rootScope: Scope,
              resultSets: Iterator[LocalScope],
              aggregates: Seq[Expression with Aggregation],
              groupFields: Seq[String]): ResultSet = {
    val groupField = groupFields.headOption.orNull
    val groupedResults = resultSets.toSeq.groupBy(_.data.find(_._1 == groupField).map(_._2).orNull)
    val results = groupedResults map { case (key, rows) =>
      val scope = LocalScope(rootScope, data = Nil)
      rows foreach { row =>
        aggregates.foreach(_.update(row))
        //println(s"$key: $row")
      }
      aggregates.map(expand(scope, _))
    }
    results
  }

  /**
    * Indicates whether this is an aggregate query
    * @return true, if at least one field is an aggregate-only field
    */
  def isAggregate: Boolean = fields.exists {
    case _: AggregateFunction => true
    case _ => false
  }

  private def filterRow(scope: LocalScope): Row = fields.map(expand(scope, _))

  private def expand(scope: Scope, expression: Expression) = {
    scope.getName(expression) -> expression.evaluate(scope).map(_.asInstanceOf[AnyRef]).orNull
  }

}

/**
  * Select Companion
  * @author lawrence.daniels@gmail.com
  */
object Select {

  /**
    * Expression Sequence Extensions
    * @param expressions the given collection of values
    */
  implicit class ExpressionSeqExtensions(val expressions: Seq[Expression]) extends AnyVal {

    @inline
    def isAllFields: Boolean = expressions.exists {
      case field: Field => field.name == "*"
      case _ => false
    }
  }

}
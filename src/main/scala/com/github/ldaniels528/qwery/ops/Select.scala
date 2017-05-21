package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.NamedExpression._
import com.github.ldaniels528.qwery.ops.Select._
import com.github.ldaniels528.qwery.sources.DataResource

import scala.language.postfixOps

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(fields: Seq[Expression],
                  source: Option[DataResource] = None,
                  condition: Option[Condition] = None,
                  groupFields: Seq[Field] = Nil,
                  orderedColumns: Seq[OrderedColumn] = Nil,
                  limit: Option[Int] = None,
                  hints: Option[Hints] = None)
  extends Executable {

  override def execute(scope: Scope): ResultSet = source.flatMap(_.getInputSource) match {
    case Some(device) =>
      val rows = device.execute(scope)
        .map(row => LocalScope(scope, row))
        .filter(rowScope => condition.isEmpty || condition.exists(_.isSatisfied(rowScope)))
        .take(limit getOrElse Int.MaxValue)

      // is this an aggregate query?
      if (isAggregate) doAggregation(scope, rows)
      else if (fields.hasAllFields) rows.map(_.row)
      else rows.map(filterRow)

    case None =>
      Iterator(fields.map(expand(scope, _)))
  }

  /**
    * Indicates whether this is an aggregate query
    * @return true, if at least one field is an aggregate-only field
    */
  def isAggregate: Boolean = fields.exists {
    case _: Aggregation => true
    case _ => false
  }

  private def doAggregation(scope: Scope, rows: Iterator[LocalScope]): ResultSet = {
    // collect the aggregates
    val groupFieldNames = groupFields.map(_.name)
    val aggregates = fields.collect {
      case agg: Aggregation => agg
      case expr@NamedExpression(name) if groupFieldNames.exists(_.equalsIgnoreCase(name)) =>
        AggregateExpression(name, expr)
    }

    // update each aggregate field, and return the evaluated results
    if (groupFields.nonEmpty) doAggregationByFields(scope, rows, aggregates, groupFieldNames)
    else {
      rows.foreach(rowScope => aggregates.foreach(_.update(rowScope)))
      Iterator(aggregates.map(expand(scope, _)))
    }
  }

  private def doAggregationByFields(parentScope: Scope,
                                    resultSets: Iterator[LocalScope],
                                    aggregates: Seq[Expression with Aggregation],
                                    groupFields: Seq[String]): ResultSet = {
    val groupField = groupFields.headOption.orNull
    val groupedResults = resultSets.toSeq.groupBy(_.row.find(_._1 == groupField).map(_._2).orNull)
    groupedResults map { case (key, rows) =>
      val scope = LocalScope(parentScope, row = Nil)
      rows foreach { row =>
        aggregates.foreach(_.update(row))
      }
      aggregates.map(expand(scope, _))
    } toIterator
  }

  private def expand(scope: Scope, expression: Expression) = {
    expression.getName -> expression.evaluate(scope).map(_.asInstanceOf[AnyRef]).orNull
  }

  private def filterRow(scope: Scope): Row = fields.map(expand(scope, _))

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
  final implicit class ExpressionSeqExtensions(val expressions: Seq[Expression]) extends AnyVal {

    @inline
    def hasAllFields: Boolean = expressions.exists {
      case field: Field => field.name == "*"
      case _ => false
    }
  }

}
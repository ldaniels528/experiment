package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.NamedExpression._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.sql.Select._

import scala.language.postfixOps

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(fields: Seq[Expression],
                  source: Option[Executable] = None,
                  condition: Option[Condition] = None,
                  groupFields: Seq[Field] = Nil,
                  joins: List[Join] = Nil,
                  orderedColumns: Seq[OrderedColumn] = Nil,
                  limit: Option[Int] = None)
  extends Executable {

  override def execute(scope: Scope): ResultSet = {
    source match {
      case Some(query) =>
        // is it a JOIN query?
        val intermediate = if (joins.isEmpty) query.execute(scope) else {
          joins.foldLeft(ResultSet()) { (_, join) => join.join(query, scope) }
        }

        // filter via the WHERE clause
        val rowScopes = intermediate.map(row => Scope(scope, row))
          .filter(rowScope => condition.isEmpty || condition.exists(_.isSatisfied(rowScope)))
          .take(limit getOrElse Int.MaxValue)

        // is this an aggregate query?
        if (isAggregate) doAggregation(scope, rowScopes)
        else if (fields.hasAllFields) ResultSet(rows = rowScopes.map(_.row))
        else ResultSet(rows = rowScopes.map(rowScope => fields.map(expand(rowScope, _))))

      case None =>
        ResultSet(rows = Iterator(fields.map(expand(scope, _))))
    }
  }

  /**
    * Indicates whether this is an aggregate query
    * @return true, if at least one field is an aggregate-only field
    */
  def isAggregate: Boolean = fields.exists {
    case _: Aggregation => true
    case _ => false
  }

  private def doAggregation(scope: Scope, rows: Iterator[Scope]): ResultSet = {
    // collect the aggregates
    val groupFieldNames = groupFields.map(_.name)
    val aggregates = fields.collect {
      case agg: Aggregation => agg
      case expr@NamedExpression(name) if groupFieldNames.exists(_.equalsIgnoreCase(name)) => AggregateExpression(name, expr)
    }

    // update each aggregate field, and return the evaluated results
    if (groupFields.nonEmpty) doAggregationByFields(scope, rows, aggregates, groupFieldNames)
    else {
      rows.foreach(rowScope => aggregates.foreach(_.update(rowScope)))
      ResultSet(rows = Iterator(aggregates.map(expand(scope, _))))
    }
  }

  private def doAggregationByFields(parentScope: Scope,
                                    resultSets: Iterator[Scope],
                                    aggregates: Seq[Expression with Aggregation],
                                    groupFields: Seq[String]): ResultSet = {
    val groupField = groupFields.headOption.orNull
    val groupedResults = resultSets.toSeq.groupBy(_.row.find(_._1.equalsIgnoreCase(groupField)).map(_._2).orNull)
    ResultSet(rows = groupedResults map { case (_, rows) =>
      rows.foreach(row => aggregates.foreach(_.update(row)))
      val scope = Scope(parentScope)
      aggregates.map(expand(scope, _))
    } toIterator)
  }

  private def expand(scope: Scope, expression: Expression) = {
    expression.getName -> expression.evaluate(scope).map(_.asInstanceOf[AnyRef]).orNull
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
  final implicit class ExpressionSeqExtensions(val expressions: Seq[Expression]) extends AnyVal {

    @inline
    def hasAllFields: Boolean = expressions.exists {
      case field: Field => field.name == "*"
      case _ => false
    }
  }

}
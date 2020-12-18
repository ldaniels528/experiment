package com.qwery.database.functions

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.ExpressionVM.RichExpression
import com.qwery.database.{KeyValues, die}
import com.qwery.models.expressions.{BasicField, Expression}

import scala.collection.mutable

/**
 * Represents the SQL DISTINCT function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to compute
 */
case class Distinct(name: String, args: List[Expression]) extends AggregateFunction {
  private var values = mutable.HashSet[Any]()
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def execute: Set[Any] = values.toSet

  override def returnType: ColumnType = expression.returnType

  override def update(keyValues: KeyValues): Unit = {
     expression match {
      case BasicField(name) => keyValues.get(name).foreach(values += _)
      case expression => die(s"Unconverted expression: $expression")
    }
  }
}
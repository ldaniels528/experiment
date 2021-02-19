package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, DoubleType}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.database.types.{QxAny, QxNull, QxNumber}
import com.qwery.models.expressions.{BasicField, Expression}

/**
 * Represents the SQL SUM function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to compute
 */
case class Sum(name: String, args: List[Expression]) extends AggregateFunction {
  private var sum: Double = 0
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def collect: Double = sum

  override def returnType: ColumnType = DoubleType

  override def append(keyValues: KeyValues): Unit = {
    expression match {
      case BasicField(name) => QxAny(keyValues.get(name)) match {
        case QxNumber(value_?) => value_?.foreach(sum += _)
        case QxNull =>
        case qxAny => die(s"Unconverted expression: $qxAny")
      }
      case expression => die(s"Unconverted expression: $expression")
    }
  }
}

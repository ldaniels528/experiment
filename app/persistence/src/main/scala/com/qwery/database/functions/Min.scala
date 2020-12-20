package com.qwery.database.functions

import com.qwery.database.ColumnTypes.{ColumnType, DoubleType}
import com.qwery.database.{KeyValues, die}
import com.qwery.database.types.{QxAny, QxNull, QxNumber}
import com.qwery.models.expressions.{BasicField, Expression}

/**
 * Represents the SQL MIN function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to compute
 */
case class Min(name: String, args: List[Expression]) extends AggregateFunction {
  private var minValue: Double = Double.MaxValue
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def collect: Double = minValue

  override def returnType: ColumnType = DoubleType

  override def append(keyValues: KeyValues): Unit = {
    expression match {
      case BasicField(name) => QxAny(keyValues.get(name)) match {
        case QxNumber(value_?) => value_?.foreach(n => minValue = n min minValue)
        case QxNull => minValue = 0.0 min minValue
        case qxAny => die(s"Unconverted expression: $qxAny")
      }
      case expression => die(s"Unconverted expression: $expression")
    }
  }
}
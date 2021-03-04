package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, DoubleType}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.database.types.{QxAny, QxNull, QxNumber}
import com.qwery.models.expressions.{BasicFieldRef, Expression}

/**
 * Represents the SQL MAX function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to compute
 */
case class Max(name: String, args: List[Expression]) extends AggregateFunction {
  private var maxValue: Double = Double.MinValue
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def collect: Double = maxValue

  override def returnType: ColumnType = DoubleType

  override def append(keyValues: KeyValues): Unit = {
    expression match {
      case BasicFieldRef(name) => QxAny(keyValues.get(name)) match {
        case QxNumber(value_?) => value_?.foreach(n => maxValue = n max maxValue)
        case QxNull => maxValue = 0.0 max maxValue
        case qxAny => die(s"Unconverted expression: $qxAny")
      }
      case expression => die(s"Unconverted expression: $expression")
    }
  }
}
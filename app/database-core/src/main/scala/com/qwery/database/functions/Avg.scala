package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, DoubleType}
import com.qwery.database.types.{QxAny, QxNull, QxNumber}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.{BasicField, Expression}

import scala.Double.NaN

/**
 * Represents the SQL AVG function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to compute
 */
case class Avg(name: String, args: List[Expression]) extends AggregateFunction {
  private var sum: Double = 0
  private var count: Double = 0
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def collect: Double = {
    if (count > 0) sum / count else NaN
  }

  override def returnType: ColumnType = DoubleType

  override def append(keyValues: KeyValues): Unit = {
    count += 1
    expression match {
      case BasicField(fname) => QxAny(keyValues.get(fname)) match {
        case QxNumber(value_?) => value_?.foreach(sum += _)
        case QxNull =>
        case qxAny => die(s"Unconverted expression '$qxAny' (${qxAny.getClass.getSimpleName})")
      }
      case expression => die(s"Unconverted expression: $expression")
    }
  }

}
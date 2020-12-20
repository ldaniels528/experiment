package com.qwery.database.functions

import com.qwery.database.ColumnTypes.{ColumnType, DoubleType}
import com.qwery.database.{KeyValues, die}
import com.qwery.database.types.{QxAny, QxNull, QxNumber}
import com.qwery.models.expressions.{BasicField, Expression}

/**
  * Represents the SQL ROUND function
  * @param name the output name of the result
  * @param args the function [[Expression arguments]] to round
  */
case class Round(name: String, args: List[Expression]) extends TransformationFunction {
  private val (expression: Expression, scale: Expression) = args match {
    case expr0 :: expr1 :: Nil => (expr0, expr1)
    case other => die(s"Exactly two arguments expected near '$other'")
  }

 override def execute(keyValues: KeyValues): Option[Double] = expression match {
    case BasicField(fname) => QxAny(keyValues.get(fname)) match {
      case QxNumber(value_?) => value_?.map(Math.round(_))
      case QxNull => None
      case qxAny => die(s"Unconverted expression '$qxAny' (${qxAny.getClass.getSimpleName})")
    }
    case expression => die(s"Unconverted expression: $expression")
  }

  override def returnType: ColumnType = DoubleType
}

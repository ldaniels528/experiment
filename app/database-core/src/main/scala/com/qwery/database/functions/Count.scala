package com.qwery.database.functions

import com.qwery.database.ColumnTypes.{ColumnType, IntType}
import com.qwery.database.{KeyValues, die}
import com.qwery.models.expressions.{AllFields, BasicField, Expression}

/**
 * Represents the SQL COUNT function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to count
 */
case class Count(name: String, args: List[Expression]) extends AggregateFunction with TransformationFunction {
  private var count: Int = 0
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def append(keyValues: KeyValues): Unit = {
    count += (expression match {
      case AllFields => 1
      case BasicField(name) => keyValues.get(name).map(_ => 1).getOrElse(0)
      case expression => die(s"Unconverted expression: $expression")
    })
  }

  override def collect: Int = count

  override def execute(keyValues: KeyValues): Option[Int] = expression match {
    case BasicField(fname) => keyValues.get(fname).map(_ => 1)
    case AllFields => Some(1)
    case expression => die(s"Unconverted expression: $expression")
  }

  override def returnType: ColumnType = IntType
}

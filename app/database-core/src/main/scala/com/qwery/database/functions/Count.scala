package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, IntType}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.{AllFields, BasicFieldRef, Expression}

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
      case BasicFieldRef(name) => keyValues.get(name).map(_ => 1).getOrElse(0)
      case expression => die(s"Unconverted expression: $expression")
    })
  }

  override def collect: Int = count

  override def execute(keyValues: KeyValues): Option[Int] = expression match {
    case BasicFieldRef(fname) => keyValues.get(fname).map(_ => 1)
    case AllFields => Some(1)
    case expression => die(s"Unconverted expression: $expression")
  }

  override def returnType: ColumnType = IntType
}

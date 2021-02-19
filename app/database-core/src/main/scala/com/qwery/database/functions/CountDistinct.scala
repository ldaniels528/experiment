package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, IntType}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.{BasicField, Expression}

import scala.collection.mutable

/**
  * Represents the SQL COUNT(DISTINCT(x)) function
  * @param name the output name of the result
  * @param args the function [[Expression arguments]] to compute
  */
case class CountDistinct(name: String, args: List[Expression]) extends AggregateFunction {
  private val values = mutable.Set[Any]()
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def append(keyValues: KeyValues): Unit = expression match {
    case BasicField(name) => keyValues.get(name).foreach(values += _)
    case expression => die(s"Unconverted expression: $expression")
  }

  override def collect: Int = values.size

  override def returnType: ColumnType = IntType
}
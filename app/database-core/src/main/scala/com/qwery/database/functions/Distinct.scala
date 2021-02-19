package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ArrayType, ColumnType}
import com.qwery.database.die
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.{BasicField, Expression}

import scala.collection.mutable

/**
  * Represents the SQL DISTINCT function
  * @param name the output name of the result
  * @param args the function [[Expression arguments]] to compute
  */
case class Distinct(name: String, args: List[Expression]) extends AggregateFunction {
  // TODO use a device to store the values
  private var values = mutable.HashSet[Any]()
  private val expression: Expression = args match {
    case expr :: Nil => expr
    case other => die(s"Too many argument near '$other'")
  }

  override def append(keyValues: KeyValues): Unit = expression match {
    case BasicField(name) => keyValues.get(name).foreach(values += _)
    case expression => die(s"Unconverted expression: $expression")
  }

  override def collect: Array[Any] = values.toArray

  override def returnType: ColumnType = ArrayType
}
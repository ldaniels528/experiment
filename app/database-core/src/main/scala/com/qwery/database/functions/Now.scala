package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.{ColumnType, LongType}
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.Expression

/**
 * Represents the SQL NOW function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]]
 */
case class Now(name: String, args: List[Expression]) extends TransformationFunction {
  assert(args.isEmpty, "No arguments were expected")

  override def execute(keyValues: KeyValues): Option[Long] = Some(System.currentTimeMillis())

  override def returnType: ColumnType = LongType
}

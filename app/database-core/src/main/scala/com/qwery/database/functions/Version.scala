package com.qwery.database.functions

import com.qwery.AppConstants
import com.qwery.database.models.ColumnTypes.{ColumnType, StringType}
import com.qwery.database.models.KeyValues
import com.qwery.models.expressions.Expression

/**
 * Represents the SQL VERSION function
 * @param name the output name of the result
 * @param args the function [[Expression arguments]] to round
 */
case class Version(name: String, args: List[Expression]) extends TransformationFunction {
  assert(args.isEmpty, "Unexpected arguments")

  override def execute(keyValues: KeyValues): Option[String] = Some(AppConstants.version)

  override def returnType: ColumnType = StringType

}

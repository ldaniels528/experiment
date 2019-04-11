package com.qwery.models.expressions

import com.qwery.models.ColumnTypes.ColumnType

/**
  * Casts the value expr to the target data type type.
  * @example cast(expr AS type)
  */
case class Cast(value: Expression, toType: ColumnType) extends Expression
package com.qwery.models

/**
 * Represents a column specification
 * @param typeName  the type name
 * @param precision the column precision
 */
case class ColumnSpec(typeName: String, precision: List[Int] = Nil)

/**
 * ColumnSpec Companion
 */
object ColumnSpec {

  object implicits {

    final implicit def stringToColumnSpec(typeName: String): ColumnSpec = ColumnSpec(typeName.toUpperCase)

  }

}
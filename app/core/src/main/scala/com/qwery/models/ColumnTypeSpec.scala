package com.qwery.models

/**
 * Represents a logical column type specification
 * @param `type`    the type name
 * @param size      the optional column size or width
 * @param precision the optional column precision
 */
case class ColumnTypeSpec(`type`: String, size: Option[Int], precision: Option[Int]) {

  def this(`type`: String) = this(`type`, size = None, precision = None)

  def this(`type`: String, size: Int) = this(`type`, size = Option(size), precision = None)

  def this(`type`: String, size: Int, precision: Int) = this(`type`, size = Option(size), precision = Option(precision))

}

/**
 * ColumnSpec Companion
 */
object ColumnTypeSpec {

  object implicits {

    final implicit def stringToColumnSpec(typeName: String): ColumnTypeSpec = new ColumnTypeSpec(typeName.toUpperCase)

  }

}
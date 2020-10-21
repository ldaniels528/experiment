package com.qwery
package models

import java.lang.{Boolean => JBoolean}

/**
 * Represents a table column
 * @param name       the column name
 * @param spec       the given [[ColumnSpec column type]]
 * @param enumValues the enumeration values (if any)
 * @param isNullable indicates whether the column may contain nulls
 * @param comment    the optional comment
 */
case class Column(name: String,
                  spec: ColumnSpec,
                  enumValues: Seq[String] = Nil,
                  isNullable: Boolean = true,
                  comment: Option[String] = None) {

  /**
   * @return true, if the column is an enumeration type
   */
  def isEnum: Boolean = enumValues.nonEmpty
}

/**
  * Column Companion
  */
object Column {

  /**
    * Constructs a new column from the given descriptor
    * @param descriptor the column descriptor (e.g. "symbol:string:true")
    * @return a new [[Column]]
    */
  def apply(descriptor: String): Column = descriptor.split("[ ]").toList match {
    case name :: _type :: nullable :: Nil =>
      new Column(name = name, spec = ColumnSpec(_type.toUpperCase), isNullable = JBoolean.valueOf(nullable))
    case name :: _type :: Nil =>
      new Column(name = name, spec = ColumnSpec(_type.toUpperCase))
    case unknown =>
      die(s"Invalid column descriptor '$unknown'")
  }

  /**
    * Column Enriched
    * @param column the given [[Column]]
    */
  final implicit class ColumnEnriched(val column: Column) extends AnyRef {
    @inline def withComment(text: String): Column = column.copy(comment = Option(text))
  }

}

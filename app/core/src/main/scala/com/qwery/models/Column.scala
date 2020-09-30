package com.qwery
package models

import java.lang.{Boolean => JBoolean}

import com.qwery.models.ColumnTypes.ColumnType

/**
  * Represents a table column
  * @param name       the column name
  * @param `type`     the given [[ColumnType column type]]
  * @param isNullable indicates whether the column may contain nulls
  * @param comment    the optional comment
  */
case class Column(name: String,
                  `type`: ColumnType = ColumnTypes.STRING,
                  isNullable: Boolean = true,
                  comment: Option[String] = None,
                  precision: List[Int] = Nil)

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
      Column(name = name, `type` = ColumnTypes.withName(_type.toUpperCase), isNullable = JBoolean.valueOf(nullable))
    case name :: _type :: Nil => Column(name = name, `type` = ColumnTypes.withName(_type.toUpperCase))
    case unknown => die(s"Invalid column descriptor '$unknown'")
  }

  /**
    * Column Enriched
    * @param column the given [[Column]]
    */
  final implicit class ColumnEnriched(val column: Column) extends AnyRef {
    @inline def withComment(text: String): Column = column.copy(comment = Option(text))
  }

}

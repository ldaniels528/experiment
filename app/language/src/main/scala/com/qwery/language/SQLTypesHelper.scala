package com.qwery.language

import com.qwery.models.ColumnTypes
import com.qwery.models.ColumnTypes._

/**
  * SQL Types Helper
  * @author lawrence.daniels@gmail.com
  */
object SQLTypesHelper {
  private val validMappings = Map(
    "array" -> ColumnTypes.ARRAY,
    "bigint" -> ColumnTypes.DOUBLE,
    "boolean" -> ColumnTypes.BOOLEAN,
    "byte" -> ColumnTypes.BYTE,
    "char" -> ColumnTypes.CHAR,
    "date" -> ColumnTypes.DATE,
    "datetime" -> ColumnTypes.TIMESTAMP,
    "decimal" -> ColumnTypes.DOUBLE,
    "double" -> ColumnTypes.DOUBLE,
    "float" -> ColumnTypes.FLOAT,
    "int" -> ColumnTypes.INTEGER,
    "integer" -> ColumnTypes.INTEGER,
    "long" -> ColumnTypes.LONG,
    "short" -> ColumnTypes.SHORT,
    "string" -> ColumnTypes.STRING,
    "timestamp" -> ColumnTypes.TIMESTAMP,
    "uuid" -> ColumnTypes.UUID
  )

  /**
    * Attempts to find a defined type corresponding to the given type name
    * @param typeName the given type name (e.g. "DECIMAL(20,2)")
    * @return the option of a [[ColumnType]]
    */
  def determineType(typeName: String): Option[ColumnType] = {
    typeName.filterNot(_.isWhitespace).toLowerCase match {
      case s if s.matches("char(\\d+)") => Some(STRING)
      case s if s.matches("decimal(\\d+)") | s.matches("decimal(\\d+,\\d+)") => Some(DOUBLE)
      case s if s.matches("double(\\d+)") | s.matches("double(\\d+,\\d+)") => Some(DOUBLE)
      case s if s.matches("numeric(\\d+)") | s.matches("numeric(\\d+,\\d+)") => Some(DOUBLE)
      case s if s.matches("text(\\d+)") => Some(STRING)
      case s if s.matches("varchar(\\d+)") => Some(STRING)
      case s => validMappings.get(s)
    }
  }

   /**
    * Indicates whether the given type name is a valid SQL type
    * @param typeName the given type name (e.g. "BigInt")
    * @return true, if the given type name is a valid SQL type
    */
  def isValidType(typeName: String): Boolean = determineType(typeName).nonEmpty

}

package com.qwery.database
package jdbc

import java.sql.ParameterMetaData

import com.qwery.database.ColumnTypes.determineValueType

/**
 * Qwery JDBC Parameter Metadata
 */
class JDBCParameterMetaData() extends ParameterMetaData with JDBCWrapper {
  private val parameters = new JDBCParameters()

  def apply(parameterNumber: Int): Any = parameters(parameterNumber)

  def clear(): Unit = parameters.clear()

  override def getParameterCount: Int = parameters.size

  def getParameters: List[Any] = parameters.toList

  override def getPrecision(parameterNumber: Int): Int = {
    val value = parameters(parameterNumber)
    val columnType = determineValueType(value)
    columnType.getFixedLength getOrElse value.toString.length
  }

  override def getScale(parameterNumber: Int): Int = {
    parameters(parameterNumber) match {
      case d: Double => BigDecimal(d).scale
      case b: BigDecimal => b.scale
      case _ => 0
    }
  }

  override def getParameterType(parameterNumber: Int): Int = determineValueType(parameters(parameterNumber)).getJDBCType()

  override def getParameterTypeName(parameterNumber: Int): String = determineValueType(parameters(parameterNumber)).toString

  override def getParameterClassName(parameterNumber: Int): String = determineValueType(parameters(parameterNumber)).getClass.getName

  override def getParameterMode(parameterNumber: Int): Int = {
    assert(parameterNumber <= parameters.size, s"Parameter index out of bounds ($parameterNumber)")
    ParameterMetaData.parameterModeIn
  }

  override def isNullable(parameterNumber: Int): Int = {
    if (parameters.isNullable(parameterNumber)) ParameterMetaData.parameterNullable else ParameterMetaData.parameterNoNulls
  }

  override def isSigned(parameterNumber: Int): Boolean = determineValueType(parameters(parameterNumber)).isSigned

  def update(parameterNumber: Int, value: Any): Unit = parameters(parameterNumber) = value

}

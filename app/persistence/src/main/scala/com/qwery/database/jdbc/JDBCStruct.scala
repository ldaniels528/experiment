package com.qwery.database
package jdbc

import java.sql.Struct
import java.util

case class JDBCStruct(typeName: String, attributes: Array[AnyRef]) extends Struct {

  override def getSQLTypeName: String = typeName

  override def getAttributes: Array[AnyRef] = attributes

  override def getAttributes(map: util.Map[String, Class[_]]): Array[AnyRef] = attributes // TODO do something about this some day

}

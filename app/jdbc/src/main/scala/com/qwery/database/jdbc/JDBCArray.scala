package com.qwery.database
package jdbc

import java.sql.ResultSet
import java.{sql, util}

import com.qwery.database.models.TableColumn

case class JDBCArray(connection: JDBCConnection, typeName: String, elements: Array[AnyRef]) extends sql.Array {

  override def getBaseTypeName: String = typeName

  override def getBaseType: Int = ColumnTypes.withName(typeName).getJDBCType

  override def getArray: AnyRef = elements

  override def getArray(map: util.Map[String, Class[_]]): AnyRef = elements

  override def getArray(index: Long, count: Int): AnyRef = elements

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): AnyRef = elements

  override def getResultSet: ResultSet = {
    val columnType = ColumnTypes.withName(typeName)
    val columns = elements.zipWithIndex map { case (_, index) =>
      TableColumn(name = f"elem$index%02d", columnType = typeName, comment = Some(s"Array element $index"),
        sizeInBytes = columnType.getFixedLength.getOrElse(255))
    }
    new JDBCResultSet(connection, databaseName = "temp", tableName = "#Array", columns = columns, data = elements.toSeq.map { elem =>
      Seq(Option(elem))
    })
  }

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet = getResultSet

  override def getResultSet(index: Long, count: Int): ResultSet = getResultSet

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet = getResultSet

  override def free(): Unit = ()

}

package com.qwery.database
package jdbc

import java.sql.ResultSetMetaData

import com.qwery.database.models.TableColumn

/**
 * Qwery Result Set MetaData
 * @param databaseName the database name
 * @param tableName    the table name
 * @param columns      the collection of [[TableColumn]]
 */
class JDBCResultSetMetaData(databaseName: String,
                            tableName: String,
                            columns: Seq[TableColumn]) extends ResultSetMetaData  with JDBCWrapper {

  override def getColumnCount: Int = columns.length

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCaseSensitive(column: Int): Boolean = true

  override def isSearchable(column: Int): Boolean = true

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    if (columns(column - 1).isNullable) 1 else 2
  }

  override def isSigned(column: Int): Boolean = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    ColumnTypes.withName(columns(column - 1).columnType).isSigned
  }

  override def getColumnDisplaySize(column: Int): Int = columns(column - 1).sizeInBytes

  override def getColumnLabel(column: Int): String = columns(column - 1).name

  override def getColumnName(column: Int): String = columns(column - 1).name

  override def getSchemaName(column: Int): String = tableName

  override def getPrecision(column: Int): Int = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    ColumnTypes.withName(columns(column - 1).columnType).getPrecision
  }

  override def getScale(column: Int): Int = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    ColumnTypes.withName(columns(column - 1).columnType).getScale
  }

  override def getTableName(column: Int): String = tableName

  override def getCatalogName(column: Int): String = databaseName

  override def getColumnType(column: Int): Int = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    ColumnTypes.withName(columns(column - 1).columnType).getJDBCType
  }

  override def getColumnTypeName(column: Int): String = columns(column - 1).columnType

  override def isReadOnly(column: Int): Boolean = false

  override def isWritable(column: Int): Boolean = true

  override def isDefinitelyWritable(column: Int): Boolean = true

  override def getColumnClassName(column: Int): String = columns(column).columnType

}

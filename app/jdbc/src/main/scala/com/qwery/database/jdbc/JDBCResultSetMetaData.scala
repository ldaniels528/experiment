package com.qwery.database
package jdbc

import com.qwery.database.models.TableColumn

import java.sql.ResultSetMetaData

/**
 * Qwery Result Set MetaData
 * @param databaseName the database name
 * @param schemaName   the schema name
 * @param tableName    the table name
 * @param columns      the collection of [[TableColumn]]
 */
class JDBCResultSetMetaData(databaseName: String,
                            schemaName: String,
                            tableName: String,
                            columns: Seq[TableColumn]) extends ResultSetMetaData with JDBCWrapper {

  override def getColumnCount: Int = columns.length

  override def isAutoIncrement(column: Int): Boolean = {
    checkColumnIndex(column)
    false
  }

  override def isCaseSensitive(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isCurrency(column: Int): Boolean = {
    checkColumnIndex(column)
    false
  }

  override def isNullable(column: Int): Int = {
    if (columns(checkColumnIndex(column)).isNullable) 1 else 2
  }

  override def isSearchable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isSigned(column: Int): Boolean = {
    ColumnTypes.withName(columns(checkColumnIndex(column)).columnType).isSigned
  }

  override def getColumnDisplaySize(column: Int): Int = columns(checkColumnIndex(column)).sizeInBytes

  override def getColumnLabel(column: Int): String = columns(checkColumnIndex(column)).name

  override def getColumnName(column: Int): String = columns(checkColumnIndex(column)).name

  override def getPrecision(column: Int): Int = {
    ColumnTypes.withName(columns(checkColumnIndex(column)).columnType).getPrecision
  }

  override def getScale(column: Int): Int = {
    ColumnTypes.withName(columns(checkColumnIndex(column)).columnType).getScale
  }

  override def getSchemaName(column: Int): String = {
    checkColumnIndex(column)
    schemaName
  }

  override def getTableName(column: Int): String = {
    checkColumnIndex(column)
    tableName
  }

  override def getCatalogName(column: Int): String = {
    checkColumnIndex(column)
    databaseName
  }

  override def getColumnType(column: Int): Int = {
    ColumnTypes.withName(columns(checkColumnIndex(column)).columnType).getJDBCType
  }

  override def getColumnTypeName(column: Int): String = columns(checkColumnIndex(column)).columnType

  override def isReadOnly(column: Int): Boolean = {
    checkColumnIndex(column)
    false
  }

  override def isWritable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def isDefinitelyWritable(column: Int): Boolean = {
    checkColumnIndex(column)
    true
  }

  override def getColumnClassName(column: Int): String = columns(checkColumnIndex(column)).columnType

  private def checkColumnIndex(column: Int): Int = {
    assert(column > 0 && column <= columns.length, "Column index out of range")
    column - 1
  }

}

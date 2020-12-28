package com.qwery.database

import java.sql.Types

import com.qwery.database.ColumnTypes.{BinaryType, BlobType, BooleanType, ByteType, ClobType, ColumnType, DateType, IntType, LongType, ShortType, StringType}

package object jdbc {
  private val mapping = Map(
    ColumnTypes.ArrayType -> Types.ARRAY,
    ColumnTypes.BigDecimalType -> Types.DECIMAL,
    ColumnTypes.BigIntType -> Types.BIGINT,
    ColumnTypes.BinaryType -> Types.VARBINARY,
    ColumnTypes.BlobType -> Types.BLOB,
    ColumnTypes.BooleanType -> Types.BOOLEAN,
    ColumnTypes.ByteType -> Types.TINYINT,
    ColumnTypes.CharType -> Types.CHAR,
    ColumnTypes.ClobType -> Types.CLOB,
    ColumnTypes.DateType -> Types.DATE,
    ColumnTypes.DoubleType -> Types.DOUBLE,
    ColumnTypes.FloatType -> Types.FLOAT,
    ColumnTypes.IntType -> Types.INTEGER,
    ColumnTypes.SerializableType -> Types.BLOB,
    ColumnTypes.LongType -> Types.BIGINT,
    ColumnTypes.ShortType -> Types.SMALLINT,
    ColumnTypes.StringType -> Types.VARCHAR,
    ColumnTypes.UUIDType -> Types.VARBINARY)

  def convertSqlToColumnType(sqlType: Int): ColumnType = {
    import java.sql.Types._
    sqlType match {
      case BIGINT => LongType
      case BINARY | VARBINARY => BinaryType
      case BOOLEAN => BooleanType
      case BLOB => BlobType
      case CLOB | NCLOB | SQLXML => ClobType
      case DATE | TIME | TIMESTAMP | TIME_WITH_TIMEZONE => DateType
      case JAVA_OBJECT | OTHER => BlobType
      case INTEGER => IntType
      case ROWID => IntType
      case SMALLINT => ShortType
      case TINYINT => ByteType
      case LONGNVARCHAR | NVARCHAR | VARCHAR => StringType
      case other => die(s"Unhandled SQL type ($other)")
    }
  }

  /**
    * JDBC Column Types Extensions
    * @param `type` the [[ColumnType]]
    */
  final implicit class JDBCColumnTypes(val `type`: ColumnType) extends AnyVal {
    @inline def getJDBCType: Int = mapping(`type`)
  }

}

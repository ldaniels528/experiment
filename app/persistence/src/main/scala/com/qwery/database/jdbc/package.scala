package com.qwery.database

import java.sql.Types

import com.qwery.database.ColumnTypes.ColumnType

package object jdbc {

  /**
   * JDBC Column Types Extensions
   * @param `type` the [[ColumnType]]
   */
  final implicit class JDBCColumnTypes(val `type`: ColumnType) extends AnyVal {

    def getJDBCType: () => Int = {
      val mapping = Map(
        ColumnTypes.BooleanType -> Types.BOOLEAN,
        ColumnTypes.ByteType -> Types.TINYINT,
        ColumnTypes.CharType -> Types.CHAR,
        ColumnTypes.DoubleType -> Types.DOUBLE,
        ColumnTypes.FloatType -> Types.FLOAT,
        ColumnTypes.IntType -> Types.INTEGER,
        ColumnTypes.LongType -> Types.BIGINT,
        ColumnTypes.ShortType -> Types.SMALLINT,
        ColumnTypes.ArrayType -> Types.ARRAY,
        ColumnTypes.BigDecimalType -> Types.DECIMAL,
        ColumnTypes.BigIntType -> Types.BIGINT,
        ColumnTypes.BinaryType -> Types.VARBINARY,
        ColumnTypes.BlobType -> Types.BLOB,
        ColumnTypes.DateType -> Types.DATE,
        ColumnTypes.StringType -> Types.VARCHAR,
        ColumnTypes.UUIDType -> Types.VARBINARY)
      () => mapping(`type`)
    }

  }

}

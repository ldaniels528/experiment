package com.qwery.models

import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.StorageFormats.StorageFormat

/**
  * Represents physical and logical tables (e.g. tables and views)
  * @author lawrence.daniels@gmail.com
  */
trait TableLike extends Invokable {
  def name: String
}

/**
  * Represents a table column
  * @param name     the column name
  * @param `type`   the given [[ColumnType column type]]
  * @param nullable indicates whether the column may contain nulls
  */
case class Column(name: String, `type`: ColumnType = ColumnTypes.STRING, nullable: Boolean = true)

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
    case name :: _type :: _nullable :: Nil => Column(name = name, `type` = ColumnTypes.withName(_type.toUpperCase), nullable = _nullable == "true")
    case name :: _type :: Nil => Column(name = name, `type` = ColumnTypes.withName(_type.toUpperCase))
    case unknown => die(s"Invalid column descriptor '$unknown'")
  }
}

/**
  * Enumeration of Column Types
  * @author lawrence.daniels@gmail.com
  */
object ColumnTypes extends Enumeration {
  type ColumnType = Value
  val BINARY, BOOLEAN, DATE, DOUBLE, INTEGER, LONG, STRING, UUID: ColumnType = Value
}

/**
  * Enumeration of Storage Formats
  * @author lawrence.daniels@gmail.com
  */
object StorageFormats extends Enumeration {
  type StorageFormat = Value
  val AVRO, CSV, JDBC, JSON, ORC, PARQUET: StorageFormat = Value
}

/**
  * Represents a view-like logical table definition
  * @param name    the name of the table
  * @param columns the table columns
  * @param source  the physical location of the data files
  * @example
  * {{{
  *     CREATE LOGICAL TABLE SpecialSecurities (Symbol STRING, price DOUBLE)
  *     FROM VALUES ('AAPL', 202), ('AMD', 22), ('INTL', 56), ('AMZN', 671)
  * }}}
  */
case class LogicalTable(name: String, columns: List[Column], source: Invokable) extends TableLike

/**
  * Represents a Hive-compatible external table definition
  * @param name            the name of the table
  * @param columns         the table columns
  * @param fieldDelimiter  the optional field delimiter (e.g. ",")
  * @param fieldTerminator the optional field terminator (e.g. ",")
  * @param inputFormat     the [[StorageFormat input format]]
  * @param outputFormat    the [[StorageFormat output format]]
  * @param location        the physical location of the data files
  * @example
  * {{{
  *     CREATE [EXTERNAL] TABLE [IF NOT EXISTS] Cars(
  *         Name STRING,
  *         Miles_per_Gallon INT,
  *         Cylinders INT,
  *         Displacement INT,
  *         Horsepower INT,
  *         Weight_in_lbs INT,
  *         Acceleration DECIMAL,
  *         Year DATE,
  *         Origin CHAR(1))
  *     COMMENT 'Data about cars from a public database'
  *     ROW FORMAT DELIMITED
  *     FIELDS TERMINATED BY ','
  *     STORED AS TEXTFILE
  *     LOCATION '/user/<username>/visdata';
  * }}}
  * @see [[https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html]]
  */
case class Table(name: String,
                 columns: List[Column],
                 fieldDelimiter: Option[String] = None,
                 fieldTerminator: Option[String] = None,
                 headersIncluded: Option[Boolean] = None,
                 nullValue: Option[String] = None,
                 properties: Option[java.util.Properties] = None,
                 inputFormat: Option[StorageFormat] = None,
                 outputFormat: Option[StorageFormat] = None,
                 location: String) extends TableLike

/**
  * Table Companion
  * @author lawrence.daniels@gmail.com
  */
object Table {

  /**
    * Creates a reference to a table by name
    * @param name the given table name
    * @return a [[TableRef table reference]]
    */
  def apply(name: String): TableRef = TableRef(name)
}

/**
  * Represents a View definition
  * @param name  the name of the view
  * @param query the given [[Invokable query]]
  * @example
  * {{{
  *   CREATE VIEW OilAndGas AS
  *   SELECT Symbol, Name, Sector, Industry, `Summary Quote`
  *   FROM Customers
  *   WHERE Industry = 'Oil/Gas Transmission'
  * }}}
  */
case class View(name: String, query: Invokable) extends TableLike
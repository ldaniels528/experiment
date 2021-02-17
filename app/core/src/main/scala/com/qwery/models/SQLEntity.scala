package com.qwery.models

import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models.expressions.Field

/**
  * Base class for all SQL entities (e.g. tables, views, functions and procedures)
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLEntity {

  /**
    * @return the name of the database entity
    */
  def name: String
}

/**
 * Base class for table-like entities (e.g. tables, inline tables and views)
 * @author lawrence.daniels@gmail.com
 */
sealed trait TableLike extends SQLEntity {

  /**
    * @return the optional description
    */
  def description: Option[String]
}

/**
  * Represents a Hive-compatible external table definition
  * @param name            the name of the table
  * @param columns         the table [[Column columns]]
  * @param ifNotExists     if true, the operation will not fail when the view exists
  * @param description     the optional description
  * @param fieldTerminator the optional field terminator/delimiter (e.g. ",")
  * @param format          the [[StorageFormat file format]]
  * @param location        the physical location of the data files
  * @example
  * {{{
  *     CREATE EXTERNAL TABLE [IF NOT EXISTS] Cars(
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
  *     LOCATION '/user/ldaniels/visdata';
  * }}}
  * @see [[https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html]]
  */
case class ExternalTable(name: String,
                         columns: List[Column],
                         ifNotExists: Boolean = false,
                         description: Option[String] = None,
                         location: Option[String] = None,
                         fieldTerminator: Option[String] = None,
                         lineTerminator: Option[String] = None,
                         headersIncluded: Option[Boolean] = None,
                         nullValue: Option[String] = None,
                         format: Option[StorageFormat] = None,
                         partitionBy: List[Column] = Nil,
                         serdeProperties: Map[String, String] = Map.empty,
                         tableProperties: Map[String, String] = Map.empty) extends TableLike

/**
  * Represents an inline table definition
  * @param name        the name of the table
  * @param columns     the table columns
  * @param values      the collection of [[Insert.Values values]]
  * @param description the optional description
  * @example
  * {{{
  *   CREATE INLINE TABLE SpecialSecurities (Symbol STRING, price DOUBLE)
  *   FROM VALUES ('AAPL', 202), ('AMD', 22), ('INTL', 56), ('AMZN', 671)
  * }}}
  */
case class InlineTable(name: String, columns: List[Column], values: Insert.Values, description: Option[String] = None) extends TableLike

/**
 * Represents an executable procedure
 * @param name   the name of the procedure
 * @param params the procedure's parameters
 * @param code   the procedure's code
 */
case class Procedure(name: String, params: Seq[Column], code: Invokable) extends SQLEntity

/**
 * Enumeration of Storage Formats
 * @author lawrence.daniels@gmail.com
 */
object StorageFormats extends Enumeration {
  type StorageFormat = Value
  val AVRO, CSV, JDBC, JSON, ORC, PARQUET: StorageFormat = Value
}

/**
  * Represents a table definition
  * @param name        the name of the table
  * @param columns     the table [[Column columns]]
  * @param ifNotExists if true, the operation will not fail when the view exists
  * @param description the optional description
  * @example
  * {{{
  *     CREATE [COLUMNAR] TABLE [IF NOT EXISTS] Cars(
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
  * }}}
  * @see [[https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html]]
  */
case class Table(name: String,
                 columns: List[Column],
                 ifNotExists: Boolean = false,
                 isColumnar: Boolean = false,
                 isPartitioned: Boolean = false,
                 description: Option[String] = None) extends TableLike

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
 * Represents a Table Index definition
 * @param name    the name of the index
 * @param table   the host [[Location table reference]]
 * @param columns the [[Field columns]] for which to build the index
 */
case class TableIndex(name: String, table: Location, columns: Seq[Field], ifNotExists: Boolean) extends SQLEntity

/**
 * Represents an enumeration type definition
 * @param name   the name of the enumeration
 * @param values the enumeration values
 * @example CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
 */
case class TypeAsEnum(name: String, values: Seq[String]) extends SQLEntity

/**
 * Represents a User-defined Function (UDF)
 * @param name        the name of the function
 * @param `class`     the fully-qualified function class name
 * @param jarLocation the optional Jar file path
 */
case class UserDefinedFunction(name: String, `class`: String, jarLocation: Option[String]) extends SQLEntity

/**
  * Represents a View definition
  * @param name        the name of the view
  * @param query       the given [[Queryable query]]
  * @param description the optional description
  * @param ifNotExists if true, the operation will not fail when the view exists
  * @example
  * {{{
  *   CREATE VIEW OilAndGas AS
  *   SELECT Symbol, Name, Sector, Industry, `Summary Quote`
  *   FROM Customers
  *   WHERE Industry = 'Oil/Gas Transmission'
  * }}}
  */
case class View(name: String,
                query: Invokable,
                description: Option[String] = None,
                ifNotExists: Boolean = false) extends TableLike with Queryable
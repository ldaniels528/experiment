package com.qwery.models

import com.qwery.models.expressions.Field

/**
  * Base class for all SQL entities (e.g. tables, views, functions and procedures)
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLEntity {

  /**
    * @return the name of the database entity
    */
  def name: String = ref.tableName

  /**
    * @return the [[TableRef fully-qualified name]]
    */
  def ref: TableRef

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
  * @param ref             the [[TableRef fully-qualified name]]
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
case class ExternalTable(ref: TableRef,
                         columns: List[Column],
                         ifNotExists: Boolean = false,
                         description: Option[String] = None,
                         location: Option[String] = None,
                         fieldTerminator: Option[String] = None,
                         lineTerminator: Option[String] = None,
                         headersIncluded: Option[Boolean] = None,
                         nullValue: Option[String] = None,
                         format: Option[String] = None,
                         partitionBy: List[Column] = Nil,
                         serdeProperties: Map[String, String] = Map.empty,
                         tableProperties: Map[String, String] = Map.empty) extends TableLike

/**
  * Represents an inline table definition
  * @param ref         the [[TableRef fully-qualified name]]
  * @param columns     the table columns
  * @param values      the collection of [[Insert.Values values]]
  * @param description the optional description
  * @example
  * {{{
  *   CREATE INLINE TABLE SpecialSecurities (Symbol STRING, price DOUBLE)
  *   FROM VALUES ('AAPL', 202), ('AMD', 22), ('INTL', 56), ('AMZN', 671)
  * }}}
  */
case class InlineTable(ref: TableRef, columns: List[Column], values: Insert.Values, description: Option[String] = None) extends TableLike

/**
  * Represents an executable procedure
  * @param ref    the [[TableRef fully-qualified name]]
  * @param params the procedure's parameters
  * @param code   the procedure's code
  */
case class Procedure(ref: TableRef, params: Seq[Column], code: Invokable) extends SQLEntity

/**
  * Represents a table definition
  * @param ref         the [[TableRef fully-qualified name]]
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
case class Table(ref: TableRef,
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
    * @param path the given table path (e.g. "securities.stocks")
    * @return a [[TableRef table reference]]
    */
  def apply(path: String): TableRef = TableRef.parse(path)

}

/**
  * Represents a Table Index definition
  * @param ref     the [[TableRef fully-qualified name]]
  * @param table   the host [[Location table reference]]
  * @param columns the [[Field columns]] for which to build the index
  */
case class TableIndex(ref: TableRef, table: Location, columns: Seq[Field], ifNotExists: Boolean) extends SQLEntity

/**
  * Represents an enumeration type definition
  * @param ref    the [[TableRef fully-qualified name]]
  * @param values the enumeration values
  * @example CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
  */
case class TypeAsEnum(ref: TableRef, values: Seq[String]) extends SQLEntity

/**
  * Represents a User-defined Function (UDF)
  * @param ref         the [[TableRef fully-qualified name]]
  * @param `class`     the fully-qualified function class name
  * @param jarLocation the optional Jar file path
  */
case class UserDefinedFunction(ref: TableRef, `class`: String, jarLocation: Option[String]) extends SQLEntity

/**
  * Represents a View definition
  * @param ref         the [[TableRef fully-qualified name]]
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
case class View(ref: TableRef,
                query: Invokable,
                description: Option[String] = None,
                ifNotExists: Boolean = false) extends TableLike with Queryable
package com.qwery.models

/**
  * Base class for all SQL entities (e.g. tables, views, functions and procedures)
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLEntity {

  /**
    * @return the [[EntityRef fully-qualified entity reference]]
    */
  def ref: EntityRef

}

/**
  * Base class for table-like entities (e.g. tables, external tables and views)
  * @author lawrence.daniels@gmail.com
  */
sealed trait TableEntity extends SQLEntity {

  /**
    * @return the optional description
    */
  def description: Option[String]

}

/**
  * Represents a Hive-compatible external table definition
  * @param ref             the [[EntityRef fully-qualified entity reference]]
  * @param columns         the table [[Column columns]]
  * @param ifNotExists     if true, the operation will not fail when the view exists
  * @param description     the optional description
  * @param fieldTerminator the optional field terminator/delimiter (e.g. ",")
  * @param format          the file format (e.g. "csv")
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
case class ExternalTable(ref: EntityRef,
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
                         tableProperties: Map[String, String] = Map.empty) extends TableEntity

/**
  * Represents an executable procedure
  * @param ref    the [[EntityRef fully-qualified entity reference]]
  * @param params the procedure's parameters
  * @param code   the procedure's code
  */
case class Procedure(ref: EntityRef, params: Seq[Column], code: Invokable) extends SQLEntity

/**
  * Represents a table definition
  * @param ref         the [[EntityRef fully-qualified entity reference]]
  * @param columns     the table [[Column columns]]
  * @param description the optional description
  * @param ifNotExists if true, the operation will not fail when the view exists
  * @example
  * {{{
  *     CREATE TABLE [IF NOT EXISTS] Cars(
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
  * @example
  * {{{
  *   CREATE TABLE SpecialSecurities (Symbol STRING, price DOUBLE)
  *   FROM VALUES ('AAPL', 202), ('AMD', 22), ('INTL', 56), ('AMZN', 671)
  * }}}
  * @see [[https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html]]
  */
case class Table(ref: EntityRef,
                 columns: List[Column],
                 description: Option[String] = None,
                 ifNotExists: Boolean = false) extends TableEntity

/**
  * Table Companion
  * @author lawrence.daniels@gmail.com
  */
object Table {

  /**
    * Creates a reference to a table by name
    * @param path the given table path (e.g. "securities.stocks")
    * @return a [[EntityRef table reference]]
    */
  def apply(path: String): EntityRef = EntityRef.parse(path)

}

/**
  * Represents a Table Index definition
  * @param ref     the [[EntityRef fully-qualified entity reference]]
  * @param table   the host [[EntityRef table reference]]
  * @param columns the columns for which to base the index
  * @param ifNotExists if true, the operation will not fail when the view exists
  */
case class TableIndex(ref: EntityRef, table: EntityRef, columns: Seq[String], ifNotExists: Boolean) extends SQLEntity {
  def indexColumnName: String = columns.head
}

/**
  * Represents an enumeration type definition
  * @param ref    the [[EntityRef fully-qualified entity reference]]
  * @param values the enumeration values
  * @example CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
  */
case class TypeAsEnum(ref: EntityRef, values: Seq[String]) extends SQLEntity

/**
  * Represents a User-defined Function (UDF)
  * @param ref         the [[EntityRef fully-qualified entity reference]]
  * @param `class`     the fully-qualified function class name
  * @param jarLocation the optional Jar file path
  */
case class UserDefinedFunction(ref: EntityRef, `class`: String, jarLocation: Option[String]) extends SQLEntity

/**
  * Represents a View definition
  * @param ref         the [[EntityRef fully-qualified entity reference]]
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
case class View(ref: EntityRef,
                query: Invokable,
                description: Option[String] = None,
                ifNotExists: Boolean = false) extends TableEntity with Queryable
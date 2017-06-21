package com.github.ldaniels528.qwery

/**
  * ops package object
  * @author lawrence.daniels@gmail.com
  */
package object ops {

  type Column = (String, Any)

  type Row = Seq[Column]

  /**
    * Row Enrichment
    * @param row the given [[Row row]] of data
    */
  final implicit class RowEnrichment(val row: Row) extends AnyVal {

    /**
      * Returns a column value by name
      * @param name the name of the desired column
      * @return the option of a value
      */
    @inline
    def get(name: String): Option[Any] = getColumn(name).map(_._2)

    /**
      * Returns a column-value pair by name
      * @param name the name of the desired column
      * @return the option of a column-value tuple
      */
    @inline
    def getColumn(name: String): Option[Column] = row.find(_._1.equalsIgnoreCase(name))

  }

}

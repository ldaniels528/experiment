package com.github.ldaniels528.qwery

/**
  * ops package object
  * @author lawrence.daniels@gmail.com
  */
package object ops {

  type Column = (String, Any)

  type Row = Seq[Column]

  type ResultSet = Iterator[Row]

  /**
    * Row Enrichmnent
    * @param row the given [[Row row]] of data
    */
  final implicit class RowEnrichment(val row: Row) extends AnyVal {

    def get(name: String): Option[Any] = row.find { case (key, _) => key == name }.map(_._2)

  }

}

package com.github.ldaniels528.qwery.ops

/**
  * Represents a row of data
  * @param columns the given collection of [[Column columns]]
  */
case class Row(columns: Seq[Column]) extends DataContainer {

  /**
    * Returns a column value by name
    * @param name the name of the desired column
    * @return the option of a value
    */
  override def get(name: String): Option[Any] = getColumn(name).map(_._2)

  /**
    * Returns a column-value pair by name
    * @param name the name of the desired column
    * @return the option of a column-value tuple
    */
  override def getColumn(name: String): Option[Column] = columns.find(_._1.equalsIgnoreCase(name))

  /**
    * Returns the number of columns
    * @return the number of columns
    */
  def size: Int = columns.size

}

/**
  * Row Companion
  * @author lawrence.daniels@gmail.com
  */
object Row {

  def apply(columns: Column): Row = new Row(Seq(columns))

  def empty: Row = Row(columns = Nil)

  implicit def rowConversion(columns: Seq[Column]): Row = Row(columns)


}

package com.github.ldaniels528.qwery.ops

/**
  * Represents a data container
  * @author lawrence.daniels@gmail.com
  */
trait DataContainer {

  /**
    * Returns a column value by name
    * @param name the name of the desired column
    * @return the option of a value
    */
  def get(name: String): Option[Any]

  /**
    * Returns a column-value pair by name
    * @param name the name of the desired column
    * @return the option of a column-value tuple
    */
  def getColumn(name: String): Option[Column]

}

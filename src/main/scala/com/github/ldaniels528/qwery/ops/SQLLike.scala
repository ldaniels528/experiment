package com.github.ldaniels528.qwery.ops

/**
  * Represents an SQL-like model
  * @author lawrence.daniels@gmail.com
  */
trait SQLLike {

  def toSQL: String

}

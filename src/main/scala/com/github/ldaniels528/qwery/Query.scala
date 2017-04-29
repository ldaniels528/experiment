package com.github.ldaniels528.qwery

/**
  * Represents an executable query
  * @author lawrence.daniels@gmail.com
  */
trait Query extends Executable {

  def condition: Option[Expression]

  def limit: Option[Int]

}

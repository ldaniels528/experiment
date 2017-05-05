package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.Expression

/**
  * Represents an executable query
  * @author lawrence.daniels@gmail.com
  */
trait Query extends Executable {

  def condition: Option[Expression]

  def limit: Option[Int]

}

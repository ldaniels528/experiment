package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.ResultSet

/**
  * Represents a query or statement
  * @author lawrence.daniels@gmail.com
  */
trait Executable extends SQLLike {

  def execute(scope: Scope): ResultSet

}

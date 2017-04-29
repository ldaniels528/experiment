package com.github.ldaniels528.qwery

import scala.collection.TraversableOnce

/**
  * Represents a Qwery executable
  * @author lawrence.daniels@gmail.com
  */
trait Executable {

  def execute(): TraversableOnce[Seq[(String, Any)]]

}

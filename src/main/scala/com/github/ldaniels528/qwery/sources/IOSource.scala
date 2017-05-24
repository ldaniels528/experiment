package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Represents an Input or Output Source
  * @author lawrence.daniels@gmail.com
  */
trait IOSource {

  def close(): Unit

  def open(scope: Scope): Unit

}

package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Row

/**
  * Output Source
  * @author lawrence.daniels@gmail.com
  */
trait OutputSource extends IOSource {

  def write(row: Row): Unit

}

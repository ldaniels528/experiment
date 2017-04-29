package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.Query

/**
  * Query Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryInputSource {

  def execute(query: Query): TraversableOnce[Map[String, String]]

}

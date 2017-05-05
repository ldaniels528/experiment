package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Query

/**
  * Query Input Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryInputSource extends QuerySource {

  def execute(query: Query): TraversableOnce[Map[String, String]]

}

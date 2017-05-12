package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Hints

/**
  * Query Output Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryOutputSource extends QuerySource {

  def open(hints: Hints): Unit

  def close(): Unit

  def flush(): Unit

  def write(data: Seq[(String, Any)]): Unit

}

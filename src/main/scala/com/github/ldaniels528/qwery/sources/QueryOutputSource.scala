package com.github.ldaniels528.qwery.sources

/**
  * Query Output Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryOutputSource extends QuerySource {

  def close(): Unit

  def flush(): Unit

  def write(data: Seq[(String, Any)]): Unit

}

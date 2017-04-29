package com.github.ldaniels528.qwery.sources

/**
  * Query Output Source
  * @author lawrence.daniels@gmail.com
  */
trait QueryOutputSource {

  def write(data: Seq[(String, Any)]): Unit

  def close(): Unit

}

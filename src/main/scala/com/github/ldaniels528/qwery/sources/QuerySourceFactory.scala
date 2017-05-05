package com.github.ldaniels528.qwery.sources

/**
  * Query Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait QuerySourceFactory {

  def understands(url: String): Boolean

}

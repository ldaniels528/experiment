package com.github.ldaniels528.qwery.sources

/**
  * Query Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait QueryInputSourceFactory extends QuerySourceFactory {

  def apply(uri: String): Option[QueryInputSource]

}

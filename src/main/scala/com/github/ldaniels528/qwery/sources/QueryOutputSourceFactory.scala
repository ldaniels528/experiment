package com.github.ldaniels528.qwery.sources

/**
  * Query Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait QueryOutputSourceFactory extends QuerySourceFactory {

  def apply(uri: String, append: Boolean): Option[QueryOutputSource]

}

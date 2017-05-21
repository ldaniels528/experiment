package com.github.ldaniels528.qwery.sources

/**
  * Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait InputSourceFactory extends IOSourceFactory {

  def apply(uri: String): Option[InputSource]

}

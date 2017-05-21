package com.github.ldaniels528.qwery.sources

/**
  * I/O Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait IOSourceFactory {

  def understands(url: String): Boolean

}

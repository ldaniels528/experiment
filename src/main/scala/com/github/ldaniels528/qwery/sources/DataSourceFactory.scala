package com.github.ldaniels528.qwery.sources

/**
  * Data Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait DataSourceFactory {
  private var inputSourceFactories = List[QueryInputSourceFactory]()
  private var outputSourceFactories = List[QueryOutputSourceFactory]()

  def +=(factory: QuerySourceFactory): Unit = {
    factory match {
      case f: QueryInputSourceFactory => inputSourceFactories = f :: inputSourceFactories
      case _ =>
    }
    factory match {
      case f: QueryOutputSourceFactory => outputSourceFactories = f :: outputSourceFactories
      case _ =>
    }
  }

  def getInputSource(url: String): Option[QueryInputSource] = {
    inputSourceFactories.find(_.understands(url)).flatMap(_.apply(url))
  }

  def getOutputSource(url: String): Option[QueryOutputSource] = {
    outputSourceFactories.find(_.understands(url)).flatMap(_.apply(url))
  }

}

/**
  * Data Source Factory Singleton
  * @author lawrence.daniels@gmail.com
  */
object DataSourceFactory extends DataSourceFactory {

  this += DelimitedInputSource
  this += DelimitedOutputSource

}
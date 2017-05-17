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

  def getInputSource(path: String): Option[QueryInputSource] = {
    inputSourceFactories.find(_.understands(path)).flatMap(_.apply(path))
  }

  def getOutputSource(path: String, append: Boolean): Option[QueryOutputSource] = {
    outputSourceFactories.find(_.understands(path)).flatMap(_.apply(path, append))
  }

}

/**
  * Data Source Factory Singleton
  * @author lawrence.daniels@gmail.com
  */
object DataSourceFactory extends DataSourceFactory {

  this += TextInputSource
  this += TextOutputSource

}
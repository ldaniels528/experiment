package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Hints

/**
  * Data Source Factory
  * @author lawrence.daniels@gmail.com
  */
trait DataSourceFactory {
  private var inputSourceFactories = List[InputSourceFactory]()
  private var outputSourceFactories = List[OutputSourceFactory]()

  def +=(factory: IOSourceFactory): Unit = {
    factory match {
      case f: InputSourceFactory => inputSourceFactories = f :: inputSourceFactories
      case _ =>
    }
    factory match {
      case f: OutputSourceFactory => outputSourceFactories = f :: outputSourceFactories
      case _ =>
    }
  }

  def getInputSource(path: String, hints: Option[Hints]): Option[InputSource] = {
    inputSourceFactories.find(_.understands(path)).flatMap(_.apply(path, hints))
  }

  def getOutputSource(path: String, append: Boolean, hints: Option[Hints]): Option[OutputSource] = {
    outputSourceFactories.find(_.understands(path)).flatMap(_.apply(path, append, hints))
  }

}

/**
  * Data Source Factory Singleton
  * @author lawrence.daniels@gmail.com
  */
object DataSourceFactory extends DataSourceFactory {

  this += InputSource
  this += OutputSource

}
package com.github.ldaniels528.qwery.sources

/**
  * Represents a Query Information Resource
  * @author lawrence.daniels@gmail.com
  */
case class QueryResource(path: String) {

  def getInputSource: Option[QueryInputSource] = DataSourceFactory.getInputSource(path)

  def getOutputSource: Option[QueryOutputSource] = DataSourceFactory.getOutputSource(path)

}


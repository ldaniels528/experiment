package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Hints

/**
  * Represents a data source resource path
  * @author lawrence.daniels@gmail.com
  */
case class DataResource(path: String) {

  def getInputSource: Option[InputSource] = DataSourceFactory.getInputSource(path)

  def getOutputSource(append: Boolean, hints: Hints): Option[OutputSource] = {
    DataSourceFactory.getOutputSource(path, append, hints)
  }

}


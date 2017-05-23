package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Scope}

/**
  * Represents a data source resource path
  * @author lawrence.daniels@gmail.com
  */
case class DataResource(path: String, hints: Option[Hints] = None) extends Executable {

  override def execute(scope: Scope): ResultSet = getInputSource match {
    case Some(device) => device.execute(scope)
    case None => Iterator.empty
  }

  def getInputSource: Option[InputSource] = {
    DataSourceFactory.getInputSource(path, hints)
  }

  def getOutputSource(append: Boolean): Option[OutputSource] = {
    DataSourceFactory.getOutputSource(path, append, hints)
  }

  override def toString: String = path

}


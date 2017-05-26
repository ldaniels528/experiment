package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Scope}

/**
  * Represents a data source resource path
  * @author lawrence.daniels@gmail.com
  */
case class DataResource(path: String, hints: Option[Hints] = None) extends Executable {

  override def execute(scope: Scope): ResultSet = getInputSource(scope) match {
    case Some(inputSource) => inputSource.execute(scope)
    case None => ResultSet()
  }

  def getInputSource(scope: Scope): Option[InputSource] = {
    DataSourceFactory.getInputSource(path = scope.expand(path), hints)
  }

  def getOutputSource(scope: Scope, append: Boolean): Option[OutputSource] = {
    DataSourceFactory.getOutputSource(path = scope.expand(path), append, hints)
  }

  override def toString: String = path

}


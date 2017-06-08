package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Scope}

/**
  * Represents a data resource path
  * @author lawrence.daniels@gmail.com
  */
case class DataResource(path: String, hints: Option[Hints] = None) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    scope.lookupView(path) match {
      case Some(view) => view.execute(scope)
      case None => getInputSource(scope).map(_.execute(scope)) getOrElse ResultSet()
    }
  }

  /**
    * Retrieves the input source that corresponds to the path
    * @param scope the given [[Scope scope]]
    * @return an option of an [[InputSource input source]]
    */
  def getInputSource(scope: Scope): Option[InputSource] = InputSource(path = scope.expand(path), hints)

  /**
    * Retrieves the output source that corresponds to the path
    * @param scope the given [[Scope scope]]
    * @return an option of an [[OutputSource output source]]
    */
  def getOutputSource(scope: Scope): Option[OutputSource] = OutputSource(path = scope.expand(path), hints)

  override def toString: String = path

}

package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Scope}

/**
  * Represents a data resource path
  * @author lawrence.daniels@gmail.com
  */
case class DataResource(path: String, hints: Option[Hints] = Some(Hints())) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    scope.lookupView(getRealPath(scope)) match {
      case Some(view) => view.execute(scope)
      case None => getInputSource(scope).map(_.execute(scope))
        .getOrElse(throw new IllegalStateException(s"No input source found for path '$path'"))
    }
  }

  /**
    * Retrieves the input source that corresponds to the path
    * @param scope the given [[Scope scope]]
    * @return an option of an [[InputSource input source]]
    */
  @inline
  def getInputSource(scope: Scope): Option[InputSource] = InputSource(path = getRealPath(scope), hints)

  /**
    * Retrieves the output source that corresponds to the path
    * @param scope the given [[Scope scope]]
    * @return an option of an [[OutputSource output source]]
    */
  @inline
  def getOutputSource(scope: Scope): Option[OutputSource] = OutputSource(path = getRealPath(scope), hints)

  /**
    * Returns the "real" (or expanded) path
    * @param scope the given [[Scope scope]]
    * @return the "real" (or expanded) path
    */
  @inline
  def getRealPath(scope: Scope): String = scope.expand(path)

}

package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.DataResource
import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Represents a query or statement
  * @author lawrence.daniels@gmail.com
  */
trait Executable {

  def execute(scope: Scope): ResultSet

}

/**
  * Executable Companion
  * @author lawrence.daniels@gmail.com
  */
object Executable {

  /**
    * Executable Enrichment
    * @param executable the given [[Executable executable]]
    */
  implicit class ExecutableEnrichment(val executable: Executable) extends AnyVal {

    @inline
    def toExpression = new Expression {
      override def evaluate(scope: Scope): Option[Any] = {
        executable.execute(scope).toSeq.headOption.flatMap(_.headOption).map(_._2)
      }
    }

    @inline
    def withHints(newHints: Option[Hints]): Executable = executable match {
      case DataResource(path, hints) => DataResource(path, newHints ?? hints)
      case exec => exec
    }
  }

}

package com.github.ldaniels528.qwery.ops

import scala.collection.JavaConverters._

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope() extends Scope {

  // import the environment variables
  System.getenv().asScala foreach { case (name, value) =>
    this += Variable(name = s"env.$name", value = Option(value))
  }

  override val row: Row = Nil

  override def get(name: String): Option[Any] = None

}
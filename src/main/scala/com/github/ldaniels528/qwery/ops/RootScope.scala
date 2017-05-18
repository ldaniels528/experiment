package com.github.ldaniels528.qwery.ops

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope() extends Scope {
  private val functions = TrieMap[String, Function]()
  private val variables = TrieMap[String, Variable]()

  // import the environment variables
  System.getenv().asScala foreach { case (name, value) =>
    variables(s"env.$name") = new Variable(Option(value))
  }

  override val data: Row = Nil

  override def get(name: String): Option[Any] = None

  override def lookup(ref: FunctionRef): Option[Function] = functions.get(ref.name)

  override def lookupVariable(name: String): Option[Variable] = variables.get(name)

}
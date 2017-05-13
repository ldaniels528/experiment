package com.github.ldaniels528.qwery.ops

import scala.collection.concurrent.TrieMap

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope() extends Scope {
  private val functions = TrieMap[String, Function]()

  override val data: Seq[(String, Any)] = Nil

  override def get(name: String): Option[Any] = None

  override def lookup(ref: FunctionRef): Option[Function] = functions.get(ref.name)

}

package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper._

import scala.collection.concurrent.TrieMap

/**
  * Represents a local ephemeral scope
  * @author lawrence.daniels@gmail.com
  */
case class LocalScope(parent: Scope, data: Row) extends Scope {
  private val functions = TrieMap[String, Function]()
  private lazy val mapping = Map(data: _*)

  override def get(name: String): Option[Any] = mapping.get(name) ?? parent.get(name)

  override def lookup(ref: FunctionRef): Option[Function] = functions.get(ref.name) ?? parent.lookup(ref)

}

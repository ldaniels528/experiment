package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Represents a local ephemeral scope
  * @author lawrence.daniels@gmail.com
  */
case class LocalScope(parent: Scope, row: Row) extends Scope {
  private lazy val mapping = Map(row: _*)

  override def get(name: String): Option[Any] = mapping.get(name) ?? parent.get(name)

  override def lookupFunction(name: String): Option[Function] = {
    super.lookupFunction(name) ?? parent.lookupFunction(name)
  }

  override def lookupVariable(name: String): Option[Variable] = {
    super.lookupVariable(name) ?? parent.lookupVariable(name)
  }

}

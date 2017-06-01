package com.github.ldaniels528.qwery.ops

/**
  * Represents a disconnect request for a connected external system
  * @param handle the handle representing the current connected state
  * @see [[Connect]]
  */
case class Disconnect(handle: String) extends Executable {

  override def execute(scope: Scope): ResultSet = ???

}

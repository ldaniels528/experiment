package com.github.ldaniels528.qwery.ops

/**
  * Connect statement
  * @example {{{ CONNECT TO 'KAFKA' WITH PROPERTIES './kafka-auth.properties' AS 'weblogs' }}}
  * @see [[Disconnect]]
  */
case class Connect(name: String, serviceName: String, hints: Option[Hints]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    ConnectionManager.lookup(serviceName) match {
      case Some(provider) => scope += provider.create(name, hints)
      case None =>
        throw new IllegalStateException(s"'$name' is not a registered service")
    }
    ResultSet.ok()
  }

}
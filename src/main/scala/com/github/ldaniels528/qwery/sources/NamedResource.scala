package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Scope}

/**
  * Represents a named resource
  * @param name     the name of the underlying resource
  * @param resource the underlying resource
  */
case class NamedResource(name: String, resource: Executable) extends Executable {

  override def execute(scope: Scope): ResultSet = resource.execute(scope)
  
}
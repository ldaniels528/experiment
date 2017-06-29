package com.github.ldaniels528.qwery.etl.triggers

import com.github.ldaniels528.qwery.ops.{ResultSet, Scope}

/**
  * Represents an event-based notification trigger
  * @author lawrence.daniels@gmail.com
  */
trait Trigger {

  def name: String

  def accepts(scope: Scope, path: String): Boolean

  def execute(scope: Scope, path: String): ResultSet

}


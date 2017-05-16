package com.github.ldaniels528.qwery.ops.builtins

import java.util.UUID

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Uuid() - Generates a random UUID
  * @author lawrence.daniels@gmail.com
  */
trait Uuid extends InternalFunction {
  override def evaluate(scope: Scope): Option[UUID] = Some(UUID.randomUUID())
}

/**
  * Uuid() function
  * @author lawrence.daniels@gmail.com
  */
object Uuid extends Uuid
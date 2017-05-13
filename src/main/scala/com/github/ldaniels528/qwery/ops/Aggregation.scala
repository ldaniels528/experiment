package com.github.ldaniels528.qwery.ops

/**
  * Represents an Aggregate Entity
  * @author lawrence.daniels@gmail.com
  */
trait Aggregation {

  def update(scope: Scope): Unit

}
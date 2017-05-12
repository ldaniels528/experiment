package com.github.ldaniels528.qwery.ops

/**
  * Represents an internal function
  * @author lawrence.daniels@gmail.com
  */
trait InternalFunction extends Expression

/**
  * Represents an internal aggregate function
  * @author lawrence.daniels@gmail.com
  */
trait AggregateFunction extends InternalFunction with Aggregation
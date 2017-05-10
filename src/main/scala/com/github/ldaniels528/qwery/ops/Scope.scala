package com.github.ldaniels528.qwery.ops

import java.util.concurrent.atomic.AtomicInteger

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {
  private val counter = new AtomicInteger()

  def data: Seq[(String, Any)]

  def get(name: String): Option[Any]

  def getName(value: Value): String = value match {
    case named: NamedValue => named.name
    case _ => s"$$${counter.addAndGet(1)}"
  }

  def lookup(ref: FunctionRef): Option[Function]

}

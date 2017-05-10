package com.github.ldaniels528.qwery.ops

import java.util.concurrent.atomic.AtomicInteger

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope(data: Seq[(String, Any)] = Nil) extends Scope {
  private lazy val mapping = Map(data: _*)
  private val counter = new AtomicInteger()

  override def get(name: String): Option[Any] = mapping.get(name)

  override def getName(value: Value): String = value match {
    case named: NamedValue => named.name
    case _ => s"$$${counter.addAndGet(1)}"
  }

}
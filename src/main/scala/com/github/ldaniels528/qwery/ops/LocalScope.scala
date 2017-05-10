package com.github.ldaniels528.qwery.ops

import java.util.concurrent.atomic.AtomicInteger

import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Represents a local ephemeral scope
  * @author lawrence.daniels@gmail.com
  */
case class LocalScope(parent: Scope, data: Seq[(String, Any)]) extends Scope {
  private val counter = new AtomicInteger()
  private lazy val mapping = Map(data: _*)

  override def get(name: String): Option[Any] = mapping.get(name) ?? parent.get(name)

  override def getName(value: Value): String = value match {
    case named: NamedValue => named.name
    case _ => s"$$${counter.addAndGet(1)}"
  }

}

package com.github.ldaniels528.qwery.ops

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
class RootScope extends Scope {
  private var data: Map[String, Any] = Map.empty

  override def get(name: String): Option[Any] = data.get(name)

  override def update(data: Map[String, Any]): Unit = this.data = data

}

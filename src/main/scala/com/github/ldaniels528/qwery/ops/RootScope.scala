package com.github.ldaniels528.qwery.ops

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope() extends Scope {
  private lazy val mapping = Map(data: _*)
  private val functions = Map[String, Function]()

  override val data: Seq[(String, Any)] = Nil

  override def get(name: String): Option[Any] = mapping.get(name)

  override def lookup(ref: FunctionRef): Option[Function] = functions.get(ref.name)

}

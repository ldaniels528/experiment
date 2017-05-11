package com.github.ldaniels528.qwery.ops

import java.util.Date

import com.github.ldaniels528.qwery.ops.RootScope._

/**
  * Represents a top-level scope
  * @author lawrence.daniels@gmail.com
  */
case class RootScope() extends Scope {
  private val functions = Map(Seq(NowFx).map(f => f.name -> f): _*)

  override val data: Seq[(String, Any)] = Nil

  override def get(name: String): Option[Any] = None

  override def lookup(ref: FunctionRef): Option[Function] = functions.get(ref.name)

}

/**
  * Root Scope
  * @author lawrence.daniels@gmail.com
  */
object RootScope {

  object NowFx extends Function {
    override def name = "NOW"

    override def invoke(scope: Scope, args: Seq[Expression]): Option[Any] = Some(new Date())
  }

}
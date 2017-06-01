package com.github.ldaniels528.qwery.ops

/**
  * Created by ldaniels3 on 6/1/2017.
  * @see [[Disconnect]]
  */
case class Connect(name: String, typeName: String, hints: Option[Hints]) extends Executable {

  override def execute(scope: Scope): ResultSet = ???

}
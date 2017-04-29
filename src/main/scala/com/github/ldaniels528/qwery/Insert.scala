package com.github.ldaniels528.qwery

/**
  * Created by ldaniels on 4/30/17.
  */
case class Insert() extends Statement {

  override def execute(): TraversableOnce[Seq[(String, Any)]] = ???

}

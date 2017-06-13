package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Executes a Stored Procedure
  * @author lawrence.daniels@gmail.com
  */
case class CallProcedure(name: String, args: Seq[Expression]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val procedure = scope.lookupProcedure(name).orDie(s"Procedure '$name' not found")
    procedure.invoke(scope, args)
  }

}

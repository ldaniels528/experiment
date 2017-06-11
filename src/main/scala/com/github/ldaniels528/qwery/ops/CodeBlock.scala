package com.github.ldaniels528.qwery.ops

/**
  * Code Block
  * @author lawrence.daniels@gmail.com
  */
case class CodeBlock(operations: Seq[Executable]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    operations.foldLeft[ResultSet](ResultSet()) { (_, operation) =>
      val localScope = LocalScope(scope, row = Nil)
      operation.execute(localScope)
    }
  }

}

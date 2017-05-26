package com.github.ldaniels528.qwery.etl.triggers

import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Scope}

/**
  * Represents a File Trigger
  * @param name        the name of the trigger
  * @param constraints the given collection of constraints
  * @param executable  the given executable
  */
case class FileTrigger(name: String, constraints: Seq[Constraint], executable: Executable) extends Trigger {

  override def accepts(scope: Scope, path: String): Boolean = constraints.forall(_.matches(path))

  override def execute(scope: Scope, path: String): ResultSet = executable.execute(scope)

}

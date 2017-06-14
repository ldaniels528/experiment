package com.github.ldaniels528.qwery.ops

/**
  * Represents a View
  * @author lawrence.daniels@gmail.com
  */
case class View(name: String, query: Executable) extends Executable {
  private var once = true

  override def execute(scope: Scope): ResultSet = {
    // called first on creation
    if (once) {
      once = !once
      scope += this
      ResultSet.affected()
    }
    else query.execute(scope)
  }

}

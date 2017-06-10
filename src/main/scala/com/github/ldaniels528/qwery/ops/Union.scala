package com.github.ldaniels528.qwery.ops

/**
  * Represents a Union operation; which combines two queries.
  * @author lawrence.daniels@gmail.com
  */
case class Union(query0: Executable, query1: Executable) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val resultSet0 = query0.execute(scope)
    val resultSet1 = query1.execute(scope)
    ResultSet(resultSet0.rows ++ resultSet1.rows, statistics = resultSet0.statistics)
  }

}

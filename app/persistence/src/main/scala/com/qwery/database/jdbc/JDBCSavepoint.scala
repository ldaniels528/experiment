package com.qwery.database.jdbc

import java.sql.Savepoint

case class JDBCSavepoint(name: String) extends Savepoint {

  override def getSavepointId: Int = name.hashCode

  override def getSavepointName: String = name

}

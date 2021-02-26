package com.qwery.models

/**
 * SQL-like DROP TABLE statement
 * @param table the given [[TableRef]]
 * @param ifExists indicates whether an existence check before attempting to delete
 * @author lawrence.daniels@gmail.com
 */
case class DropTable(table: TableRef, ifExists: Boolean) extends Invokable {
  override def toString: String = s"${getClass.getSimpleName}($table)"
}

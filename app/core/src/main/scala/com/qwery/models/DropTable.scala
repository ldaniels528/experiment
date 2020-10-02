package com.qwery.models

/**
 * SQL-like DROP TABLE statement
 * @param tableRef the given [[TableRef]]
 * @author lawrence.daniels@gmail.com
 */
case class DropTable(tableRef: TableRef) extends Invokable {
  override def toString: String = s"${getClass.getSimpleName}($tableRef)"
}

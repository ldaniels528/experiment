package com.qwery.database.server

import com.qwery.models._
import org.slf4j.LoggerFactory

/**
 * Table Query Executor
 */
object TableQueryExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  type Result = Seq[(String, Any)]

  def invoke(select: Select)(implicit tableManager: TableManager): Seq[Result] = {
    select.from match {
      case Some(TableRef(tableName)) =>
        val table = tableManager.get(tableName)
        select.fields
        select.orderBy
        table.search(limit = select.limit,
          (select.where match {
            case Some(condition) =>
              throw new IllegalArgumentException(s"Unhandled condition $condition")
            case None => Nil
          }): _*)
      case Some(queryable) =>
        logger.info(s"queryable = $queryable")
        ???
      case None => ???
    }
  }

  final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
    def translate(implicit tableManager: TableManager): Seq[Result] = invokable match {
      case select: Select => invoke(select)
      case unknown => throw new IllegalArgumentException(s"Unhandled operation $unknown")
    }
  }

}

package com.qwery.models

import com.qwery.models.expressions.Condition

/**
 * SQL DELETE
 * @param table the [[TableRef]]
 * @param where the optional [[Condition condition]]
 * @param limit the limit
 */
case class Delete(table: TableRef,
                  where: Option[Condition] = None,
                  limit: Option[Int] = None) extends Invokable
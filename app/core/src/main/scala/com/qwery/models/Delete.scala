package com.qwery.models

import com.qwery.models.expressions.Condition

/**
 * SQL DELETE
 * @param table the [[EntityRef]]
 * @param where the optional [[Condition condition]]
 * @param limit the limit
 */
case class Delete(table: EntityRef,
                  where: Option[Condition] = None,
                  limit: Option[Int] = None) extends Invokable
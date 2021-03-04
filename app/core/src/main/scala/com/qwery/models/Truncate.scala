package com.qwery.models

/**
 * Represents a SQL TRUNCATE statement
 * @param table the [[EntityRef table]] to update
 */
case class Truncate(table: EntityRef) extends Invokable
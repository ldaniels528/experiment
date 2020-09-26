package com.qwery.models

/**
 * Represents a SQL TRUNCATE statement
 * @param table the [[TableRef table]] to update
 */
case class Truncate(table: Invokable) extends Invokable
package com.qwery.models

/**
  * Represents a code block (e.g. BEGIN ... END)
  * @param statements one or more [[Invokable statements]] to execute
  */
case class SQL(statements: List[Invokable]) extends Invokable

/**
  * SQL Companion
  * @author lawrence.daniels@gmail.com
  */
object SQL {

  /**
    * Returns an SQL code block containing the given operations
    * @param operations the given collection of [[Invokable]]
    * @return the [[SQL code block]]
    */
  def apply(operations: Invokable*): SQL = new SQL(operations.toList)

}
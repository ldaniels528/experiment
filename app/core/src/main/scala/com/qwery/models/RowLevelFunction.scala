package com.qwery.models

import com.qwery.models.expressions.Expression

/**
  * Base trait for all row-level functions
  * @author lawrence.daniels@gmail.com
  */
sealed trait RowLevelFunction extends Invokable

/**
  * Retrieves files from the local filesystem
  * @param path the local file path
  * @example {{{ SELECT * FROM (FileSystem('~/Downloads')) WHERE name LIKE '%.csv' }}}
  */
case class FileSystem(path: String) extends RowLevelFunction with Aliasable

/**
  * Invokes a procedure by name
  * @param name the name of the procedure
  * @param args the collection of [[Expression arguments]] to be passed to the procedure upon invocation
  * @example {{{ CALL getEligibleFeeds("/home/ubuntu/feeds/") }}}
  */
case class ProcedureCall(name: String, args: List[Expression]) extends RowLevelFunction with Aliasable

/**
  * RETURN statement
  * @param value the given return value
  * @example {{{ RETURN @rowSet }}}
  */
case class Return(value: Option[Invokable]) extends RowLevelFunction
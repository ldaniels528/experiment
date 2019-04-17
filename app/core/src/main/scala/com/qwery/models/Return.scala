package com.qwery.models

/**
  * RETURN statement
  * @param value the given return value
  * @example {{{ RETURN @rowSet }}}
  */
case class Return(value: Option[Invokable]) extends Invokable
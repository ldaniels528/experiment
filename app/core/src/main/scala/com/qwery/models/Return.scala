package com.qwery.models

/**
  * RETURN statement
  * @param value the given return value
  */
case class Return(value: Option[Invokable]) extends Invokable
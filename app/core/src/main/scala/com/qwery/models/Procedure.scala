package com.qwery.models

/**
  * Represents an executable procedure
  * @param name   the name of the procedure
  * @param params the procedure's parameters
  * @param code   the procedure's code
  */
case class Procedure(name: String, params: Seq[Column], code: Invokable) extends Invokable

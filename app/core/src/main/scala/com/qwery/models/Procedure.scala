package com.qwery.models

/**
  * Represents an executable procedure
  * @param name
  * @param params
  * @param code
  */
case class Procedure(name: String, params: Seq[Column], code: Invokable) extends Invokable



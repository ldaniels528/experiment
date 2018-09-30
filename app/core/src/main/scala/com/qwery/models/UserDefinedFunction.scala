package com.qwery.models

/**
  * User Defined Function (UDF)
  * @param name the name of the function
  */
case class UserDefinedFunction(name: String, `class`: String, jar: Option[String]) extends Invokable with Aliasable
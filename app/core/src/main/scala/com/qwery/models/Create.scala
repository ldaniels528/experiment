package com.qwery.models

/**
  * SQL-like CREATE statement
  * @param entity the given [[SQLEntity]]
  * @author lawrence.daniels@gmail.com
  */
case class Create(entity: SQLEntity) extends Invokable


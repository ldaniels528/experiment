package com.qwery.models

/**
  * Row Variable assignment
  * @param name    the given variable name
  * @param dataset the given [[Queryable dataset]]
  */
case class SetRowVariable(name: String, dataset: Invokable) extends Invokable
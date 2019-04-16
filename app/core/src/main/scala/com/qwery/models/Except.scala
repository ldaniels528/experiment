package com.qwery.models

/**
  * Represents a Subtract operation; which returns the intersection of two queries.
  * @param query0 the first query
  * @param query1 the second query
  */
case class Except(query0: Invokable, query1: Invokable) extends Invokable with Aliasable
package com.qwery.models

/**
  * Represents a Union operation; which combines two queries.
  * @param query0     the first query
  * @param query1     the second query
  * @param isDistinct indicates wthether the results should be distinct
  */
case class Union(query0: Invokable, query1: Invokable, isDistinct: Boolean = false) extends Invokable with Aliasable
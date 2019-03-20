package com.qwery.models

/**
  * Represents an enumeration of JOIN types
  * @author lawrence.daniels@gmail.com
  */
object JoinTypes extends Enumeration {
  type JoinType = Value
  val CROSS, INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER: JoinType = Value
}
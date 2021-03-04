package com.qwery.models

import com.qwery.models.JoinTypes.JoinType
import com.qwery.models.expressions.Condition

/**
  * Base class for all Join expressions
  * @author lawrence.daniels@gmail.com
  */
sealed trait Join {

  /**
    * @return the [[EntityRef table]] or [[Invokable query]]
    */
  def source: Invokable

  /**
    * @return the [[JoinType join type]] (e.g. "INNER")
    */
  def `type`: JoinType

}

/**
  * Join Companion
  * @author lawrence.daniels@gmail.com
  */
object Join {

  /**
    * Creates a JOIN ON model
    * @param source    the [[EntityRef table]] or [[Invokable query]]
    * @param condition the [[Condition conditional expression]]
    * @param `type`    the given [[JoinType]]
    */
  def apply(source: Invokable, condition: Condition, `type`: JoinType): Join = Join.On(source, condition, `type`)

  /**
    * Creates a JOIN USING model
    * @param source  the [[EntityRef table]] or [[Invokable query]]
    * @param columns the given columns for which to join
    * @param `type`  the given [[JoinType]]
    */
  def apply(source: Invokable, columns: Seq[String], `type`: JoinType): Join = Join.Using(source, columns, `type`)

  /**
    * Represents a JOIN ON clause
    * @param source    the [[EntityRef table]] or [[Invokable query]]
    * @param condition the [[Condition conditional expression]]
    * @param `type`    the given [[JoinType]]
    */
  case class On(source: Invokable, condition: Condition, `type`: JoinType) extends Join

  /**
    * Represents a JOIN USING clause
    * @param source  the [[EntityRef table]] or [[Invokable query]]
    * @param columns the given columns for which to join
    * @param `type`  the given [[JoinType]]
    */
  case class Using(source: Invokable, columns: Seq[String], `type`: JoinType) extends Join

}



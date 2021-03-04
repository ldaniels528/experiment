package com.qwery.models

import com.qwery.models.Insert._
import com.qwery.models.expressions.{Expression, FieldRef}

/**
  * Represents a SQL-like Insert statement
  * @param destination the given [[Destination destination]]
  * @param source      the given [[Queryable source]]
  * @param fields      the given collection of [[FieldRef]]s
  */
case class Insert(destination: Destination, source: Queryable, fields: Seq[FieldRef] = Nil) extends Invokable

/**
  * Insert Companion
  * @author lawrence.daniels@gmail.com
  */
object Insert {

  /**
    * Represents a row of data
    */
  type DataRow = List[Expression]

  /**
    * Represents a write mode
    * @author lawrence.daniels@gmail.com
    */
  sealed trait Destination extends Invokable {

    def target: EntityRef

    def isAppend: Boolean

    final def isOverwrite: Boolean = !isAppend

  }

  /**
    * Represents an Append Write Mode
    * @param target the given [[EntityRef]]
    */
  case class Into(target: EntityRef) extends Destination {
    override def isAppend: Boolean = true
  }

  /**
    * Represents a Overwrite Write Mode
    * @param target the given [[EntityRef]]
    */
  case class Overwrite(target: EntityRef) extends Destination {
    override def isAppend: Boolean = false
  }

  /**
    * Represents a static insert values collection
    * @param values the given collection of [[DataRow row]]s
    */
  case class Values(values: List[DataRow]) extends Queryable

}
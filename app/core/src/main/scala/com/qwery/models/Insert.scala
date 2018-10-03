package com.qwery.models

import com.qwery.models.Insert._
import com.qwery.models.expressions.{Expression, Field}

/**
  * Represents a SQL-like Insert statement
  * @param destination the given [[Destination destination]]
  * @param source      the given [[Invokable source]]
  *                    @param fields the given collection of [[Field]]s
  */
case class Insert(destination: Destination, source: Invokable, fields: List[Field] = Nil) extends Invokable

/**
  * Insert Companion
  * @author lawrence.daniels@gmail.com
  */
object Insert {

  type DataRow = List[Expression]

  /**
    * Represents a write mode
    * @author lawrence.daniels@gmail.com
    */
  sealed trait Destination extends Invokable {
    def target: Location
  }

  /**
    * Represents an Append Write Mode
    * @author lawrence.daniels@gmail.com
    */
  case class Into(target: Location) extends Destination

  /**
    * Represents a Overwrite Write Mode
    * @author lawrence.daniels@gmail.com
    */
  case class Overwrite(target: Location) extends Destination

  /**
    * Represents a static insert values collection
    * @param values the given insert values
    */
  case class Values(values: List[DataRow]) extends Invokable

}
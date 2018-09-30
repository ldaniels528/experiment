package com.qwery.models

/**
  * SQL-like CREATE statement
  * @author lawrence.daniels@gmail.com
  */
case class Create(entity: Invokable) extends Invokable

/**
  * Create Companion
  * @author lawrence.daniels@gmail.com
  */
object Create {

  /**
    * CREATE TABLE statement
    * @param table the given [[Table]]
    * @return a new [[Create create]] statement
    */
  def apply(table: Table) = new Create(table)

  /**
    * CREATE TABLE statement
    * @param view the given [[View]]
    * @return a new [[Create create]] statement
    */
  def apply(view: View) = new Create(view)

  /**
    * CREATE TABLE statement
    * @param function the given [[UserDefinedFunction]]
    * @return a new [[Create create]] statement
    */
  def apply(function: UserDefinedFunction) = new Create(function)

}
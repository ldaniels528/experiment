package com.qwery.platform

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {
  private val logger = LoggerFactory.getLogger(getClass)
  private val variables = TrieMap[String, Any]()

  /**
    * Attempts to retrieve the desired variable by name
    * @param name the given variable name
    * @return the option of a value
    */
  def apply(name: String): Option[Any] = variables.get(name)

  /**
    * Indicated whether a variable exists by name
    * @param name the given variable name
    * @return true, if the variable was found in this scope
    */
  def contains(name: String): Boolean = variables.exists { case (key, _) => key equalsIgnoreCase name }

  /**
    * Updates the state of a variable by name
    * @param name  the name of the variable to update
    * @param value the updated value
    */
  def update(name: String, value: Any): Unit = {
    logger.info(s"Setting variable '$name' to '$value'")
    variables(name) = value
  }

  override def toString = s"Scope(${variables.mkString(",")})"

}

/**
  * Scope Companion
  * @author lawrence.daniels@gmail.com
  */
object Scope {

  /**
    * Creates a new scope
    * @return the [[Scope]]
    */
  def apply(): Scope = new Scope {}

}
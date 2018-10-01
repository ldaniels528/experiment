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
    * Updates the state of a variable by name
    * @param name  the name of the variable to update
    * @param value the updated value
    */
  def update(name: String, value: Any): Unit = {
    logger.info(s"Setting variable '$name' to '$value'")
    variables(name) = value
  }

}

/**
  * Scope Companion
  * @author lawrence.daniels@gmail.com
  */
object Scope {
  def apply(): Scope = new Scope {}
}
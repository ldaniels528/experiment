package com.qwery
package models

import com.qwery.models.expressions.LocalVariableRef

/**
  * Represents a location of a data set (e.g. table or file)
  * @author lawrence.daniels@gmail.com
  */
sealed trait Location extends Queryable with Aliasable

/**
  * Represents a reference to a [[Table table]]
  * @param databaseName the optional name of the database
  * @param schemaName   the optional name of the schema
  * @param tableName    the name of the table
  */
case class TableRef(databaseName: Option[String], schemaName: Option[String], tableName: String) extends Location {

  /**
    * Creates a reference to a [[Table table]]
    * @param databaseName the optional name of the database
    * @param schemaName   the optional name of the schema
    * @param tableName    the name of the table
    */
  def this(databaseName: String, schemaName: String, tableName: String) = this(Option(databaseName), Option(schemaName), tableName)

  override def toString: String = toSQL

  def toSQL: String = {
    val sb = new StringBuilder()
    databaseName.foreach(name => sb.append(name).append('.'))
    schemaName.foreach(name => sb.append(name).append('.'))
    sb.append(tableName).toString()
  }

  def withDatabase(databaseName: String): TableRef = this.copy(databaseName = Option(databaseName))

  def withSchema(schemaName: String): TableRef = this.copy(schemaName = Option(schemaName))

}

/**
  * Table Reference Companion
  */
object TableRef {

  def parse(path: String): TableRef = {
    val (databaseName, schemaName, tableName) = path.split("[.]").toList match {
      case databaseName :: schemaName :: tableName :: Nil => (Option(databaseName), Option(schemaName), tableName)
      case schemaName :: tableName :: Nil => (None, Option(schemaName), tableName)
      case tableName :: Nil => (None, None, tableName)
      case _ => die(s"'$path' is an invalid database path")
    }
    new TableRef(databaseName, schemaName, tableName)
  }

}

/**
  * Represents the variable location path (e.g. '@s3Path')
  * @param variable the given [[LocalVariableRef local variable]]
  */
case class VariableLocationRef(variable: LocalVariableRef) extends Location
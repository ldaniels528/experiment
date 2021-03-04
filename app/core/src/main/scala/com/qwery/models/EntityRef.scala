package com.qwery
package models

/**
  * Represents a reference to a [[Table table]]
  * @param databaseName the optional name of the database
  * @param schemaName   the optional name of the schema
  * @param name         the name of the table
  */
case class EntityRef(databaseName: Option[String], schemaName: Option[String], name: String) extends Queryable with Aliasable {

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
    sb.append(name).toString()
  }

  def withDatabase(databaseName: String): EntityRef = this.copy(databaseName = Option(databaseName))

  def withSchema(schemaName: String): EntityRef = this.copy(schemaName = Option(schemaName))

}

/**
  * Table Reference Companion
  */
object EntityRef {

  def parse(path: String): EntityRef = {
    val (databaseName, schemaName, tableName) = path.split("[.]").toList match {
      case databaseName :: schemaName :: tableName :: Nil => (Option(databaseName), Option(schemaName), tableName)
      case schemaName :: tableName :: Nil => (None, Option(schemaName), tableName)
      case tableName :: Nil => (None, None, tableName)
      case _ => die(s"'$path' is an invalid database path")
    }
    new EntityRef(databaseName, schemaName, tableName)
  }

}

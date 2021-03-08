package com.qwery.database.models

/**
  * Represents a table search result
  * @param databaseName the name of the database
  * @param schemaName   the name of the schema
  * @param tableName    the name of the table
  * @param tableType    the table type
  * @param description  the table description
  */
case class TableSearchResult(databaseName: String, schemaName: String, tableName: String, tableType: String, description: Option[String])

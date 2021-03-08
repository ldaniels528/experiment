package com.qwery.database.models

/**
  * Represents a Database Summary
  * @param databaseName the database name
  * @param tables the collection of [[TableSummary table summaries]]
  */
case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary])

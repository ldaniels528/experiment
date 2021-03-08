package com.qwery.database.models

/**
  * Represents a Table Summary
  * @param tableName        the table name
  * @param schemaName       the schema name
  * @param tableType        the table type
  * @param description      the table description
  * @param lastModifiedTime the table's last modified time
  * @param fileSize         the table's file size
  * @param href             the table's HTTP URL
  */
case class TableSummary(tableName: String,
                        schemaName: String,
                        tableType: String,
                        description: Option[String],
                        lastModifiedTime: String,
                        fileSize: Long,
                        href: Option[String] = None)

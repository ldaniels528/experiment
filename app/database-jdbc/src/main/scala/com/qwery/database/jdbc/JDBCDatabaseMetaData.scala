package com.qwery.database
package jdbc

import com.qwery.database.models.ColumnTypes._
import com.qwery.database.files.DatabaseManagementSystem
import com.qwery.database.models.{Column, ColumnMetadata}

import java.sql.{DatabaseMetaData, ResultSet, ResultSetMetaData, RowIdLifetime}
import scala.beans.{BeanProperty, BooleanBeanProperty}

/**
 * Qwery Database Metadata
 * @param connection the [[JDBCConnection connection]]
 * @param URL        the JDBC Connection URL (e.g. "jdbc:qwery://localhost:8233/securities")
 */
class JDBCDatabaseMetaData(@BeanProperty val connection: JDBCConnection, @BeanProperty val URL: String)
  extends DatabaseMetaData with JDBCWrapper {
  private val rights = Seq("SELECT", "INSERT", "UPDATE", "DELETE", "DROP")

  @BeanProperty val catalogSeparator: String = "."
  @BeanProperty val catalogTerm: String = "DATABASE"
  @BeanProperty val databaseMajorVersion: Int = 0
  @BeanProperty val databaseMinorVersion: Int = 1
  @BeanProperty val databaseProductName: String = "Qwery"
  @BeanProperty val databaseProductVersion: String = s"$databaseMajorVersion.$databaseMinorVersion"
  @BeanProperty val defaultTransactionIsolation: Int = ResultSet.CONCUR_UPDATABLE
  @BeanProperty val driverMajorVersion: Int = 0
  @BeanProperty val driverMinorVersion: Int = 2
  @BeanProperty val driverVersion: String = s"$driverMajorVersion.$driverMinorVersion"
  @BeanProperty val driverName: String = s"Qwery v$driverVersion"
  @BeanProperty val extraNameCharacters: String = ""
  @BeanProperty val numericFunctions: String =
    """|ABS COS ROUND SIN
       |""".stripMargin.trim.replaceAllLiterally("\n", " ")
  @BeanProperty val JDBCMajorVersion: Int = 0
  @BeanProperty val JDBCMinorVersion: Int = 1
  @BeanProperty val identifierQuoteString: String = "`"
  @BeanProperty val maxBinaryLiteralLength: Int = Int.MaxValue
  @BeanProperty val maxCharLiteralLength: Int = Int.MaxValue
  @BeanProperty val maxColumnNameLength: Int = 128
  @BeanProperty val maxColumnsInGroupBy: Int = 1
  @BeanProperty val maxColumnsInIndex: Int = 1
  @BeanProperty val maxColumnsInOrderBy: Int = 1
  @BeanProperty val maxColumnsInSelect: Int = Int.MaxValue
  @BeanProperty val maxColumnsInTable: Int = Int.MaxValue
  @BeanProperty val maxConnections: Int = Int.MaxValue
  @BeanProperty val maxCursorNameLength: Int = 128
  @BeanProperty val maxIndexLength: Int = Int.MaxValue
  @BeanProperty val maxSchemaNameLength: Int = 128
  @BeanProperty val maxProcedureNameLength: Int = 128
  @BeanProperty val maxCatalogNameLength: Int = 128
  @BeanProperty val maxRowSize: Int = Int.MaxValue
  @BeanProperty val maxStatementLength: Int = Int.MaxValue
  @BeanProperty val maxStatements: Int = Short.MaxValue
  @BeanProperty val maxTableNameLength: Int = 128
  @BeanProperty val maxTablesInSelect: Int = Short.MaxValue
  @BeanProperty val maxUserNameLength: Int = 128
  @BeanProperty val procedureTerm: String = "PROCEDURE"
  @BooleanBeanProperty var readOnly: Boolean = false
  @BeanProperty val resultSetHoldability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
  @BeanProperty val rowIdLifetime: RowIdLifetime = RowIdLifetime.ROWID_VALID_FOREVER
  @BeanProperty val schemaTerm: String = "SCHEMA"
  @BeanProperty val searchStringEscape: String = "\\"
  @BeanProperty val stringFunctions: String =
    """|LEN LTRIM RTRIM SUBSTR SUBSTRING TRIM
       |""".stripMargin.trim.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")
  @BeanProperty val systemFunctions: String =
    """|NOW
       |""".stripMargin.trim.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")
  @BeanProperty val SQLKeywords: String =
    """|AND AS BETWEEN CAST DATABASE DELETE FROM IN INSERT INTO IS NOT NULL ON OR REPLACE SELECT TABLE UPDATE VALUES WHERE
       |""".stripMargin.trim.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")
  @BeanProperty var SQLStateType: Int = _
  @BeanProperty val timeDateFunctions: String =
    """|NOW DATEDIFF
       |""".stripMargin.trim.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")
  @BeanProperty val userName: String = ""

  override def allProceduresAreCallable(): Boolean = true

  override def allTablesAreSelectable(): Boolean = true

  override def generatedKeyAlwaysReturned(): Boolean = true

  override def nullsAreSortedHigh(): Boolean = true

  override def nullsAreSortedLow(): Boolean = !nullsAreSortedHigh()

  override def nullsAreSortedAtStart(): Boolean = false

  override def nullsAreSortedAtEnd(): Boolean = !nullsAreSortedAtStart()

  override def usesLocalFiles(): Boolean = true

  override def usesLocalFilePerTable(): Boolean = true

  override def supportsMixedCaseIdentifiers(): Boolean = true

  override def storesUpperCaseIdentifiers(): Boolean = false

  override def storesLowerCaseIdentifiers(): Boolean = false

  override def storesMixedCaseIdentifiers(): Boolean = true

  override def supportsMixedCaseQuotedIdentifiers(): Boolean = true

  override def storesUpperCaseQuotedIdentifiers(): Boolean = false

  override def storesLowerCaseQuotedIdentifiers(): Boolean = false

  override def storesMixedCaseQuotedIdentifiers(): Boolean = true

  override def supportsAlterTableWithAddColumn(): Boolean = true

  override def supportsAlterTableWithDropColumn(): Boolean = true

  override def supportsColumnAliasing(): Boolean = true

  override def nullPlusNonNullIsNull(): Boolean = true

  override def supportsConvert(): Boolean = true

  override def supportsConvert(fromType: Int, toType: Int): Boolean = {
    import java.sql.Types._
    (fromType, toType) match {
      case (VARCHAR, _) => true
      case (_, VARCHAR) => true
      case (a, b) if a == b => true
      case _ => false
    }
  }

  override def supportsTableCorrelationNames(): Boolean = true

  override def supportsDifferentTableCorrelationNames(): Boolean = true

  override def supportsExpressionsInOrderBy(): Boolean = false

  override def supportsOrderByUnrelated(): Boolean = false

  override def supportsGroupBy(): Boolean = true

  override def supportsGroupByUnrelated(): Boolean = false

  override def supportsGroupByBeyondSelect(): Boolean = false

  override def supportsLikeEscapeClause(): Boolean = true

  override def supportsMultipleResultSets(): Boolean = true

  override def supportsMultipleTransactions(): Boolean = false

  override def supportsNonNullableColumns(): Boolean = false

  override def supportsMinimumSQLGrammar(): Boolean = true

  override def supportsCoreSQLGrammar(): Boolean = false

  override def supportsExtendedSQLGrammar(): Boolean = false

  override def supportsANSI92EntryLevelSQL(): Boolean = false

  override def supportsANSI92IntermediateSQL(): Boolean = false

  override def supportsANSI92FullSQL(): Boolean = false

  override def supportsIntegrityEnhancementFacility(): Boolean = false

  override def supportsOuterJoins(): Boolean = false

  override def supportsFullOuterJoins(): Boolean = false

  override def supportsLimitedOuterJoins(): Boolean = false

  override def isCatalogAtStart: Boolean = false

  override def supportsSchemasInDataManipulation(): Boolean = true

  override def supportsSchemasInProcedureCalls(): Boolean = true

  override def supportsSchemasInTableDefinitions(): Boolean = true

  override def supportsSchemasInIndexDefinitions(): Boolean = true

  override def supportsSchemasInPrivilegeDefinitions(): Boolean = true

  override def supportsCatalogsInDataManipulation(): Boolean = true

  override def supportsCatalogsInProcedureCalls(): Boolean = true

  override def supportsCatalogsInTableDefinitions(): Boolean = true

  override def supportsCatalogsInIndexDefinitions(): Boolean = true

  override def supportsCatalogsInPrivilegeDefinitions(): Boolean = false

  override def supportsPositionedDelete(): Boolean = true

  override def supportsPositionedUpdate(): Boolean = true

  override def supportsSelectForUpdate(): Boolean = true

  override def supportsStoredProcedures(): Boolean = true

  override def supportsSubqueriesInComparisons(): Boolean = false

  override def supportsSubqueriesInExists(): Boolean = false

  override def supportsSubqueriesInIns(): Boolean = false

  override def supportsSubqueriesInQuantifieds(): Boolean = true

  override def supportsCorrelatedSubqueries(): Boolean = true

  override def supportsUnion(): Boolean = true

  override def supportsUnionAll(): Boolean = false

  override def supportsOpenCursorsAcrossCommit(): Boolean = false

  override def supportsOpenCursorsAcrossRollback(): Boolean = false

  override def supportsOpenStatementsAcrossCommit(): Boolean = false

  override def supportsOpenStatementsAcrossRollback(): Boolean = false

  override def doesMaxRowSizeIncludeBlobs(): Boolean = true

  override def supportsTransactions(): Boolean = false

  override def supportsTransactionIsolationLevel(level: Int): Boolean = false

  override def supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

  override def supportsDataManipulationTransactionsOnly(): Boolean = false

  override def dataDefinitionCausesTransactionCommit(): Boolean = false

  override def dataDefinitionIgnoredInTransactions(): Boolean = false

  override def supportsResultSetType(`type`: Int): Boolean = true

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean = true

  override def ownUpdatesAreVisible(`type`: Int): Boolean = true

  override def ownDeletesAreVisible(`type`: Int): Boolean = true

  override def ownInsertsAreVisible(`type`: Int): Boolean = true

  override def othersUpdatesAreVisible(`type`: Int): Boolean = true

  override def othersDeletesAreVisible(`type`: Int): Boolean = true

  override def othersInsertsAreVisible(`type`: Int): Boolean = true

  override def updatesAreDetected(`type`: Int): Boolean = false

  override def deletesAreDetected(`type`: Int): Boolean = false

  override def insertsAreDetected(`type`: Int): Boolean = false

  override def supportsBatchUpdates(): Boolean = true

  override def supportsSavepoints(): Boolean = false

  override def supportsNamedParameters(): Boolean = false

  override def supportsMultipleOpenResults(): Boolean = true

  override def supportsGetGeneratedKeys(): Boolean = false

  override def supportsResultSetHoldability(holdability: Int): Boolean = true

  override def locatorsUpdateCopy(): Boolean = true

  override def supportsStatementPooling(): Boolean = false

  override def supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  override def autoCommitFailureClosesAllResultSets(): Boolean = false

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String, attributeNamePattern: String): ResultSet =  {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "ATTR_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "ATTR_TYPE_NAME", columnType = StringType),
      mkColumn(name = "ATTR_SIZE", columnType = IntType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType),
      mkColumn(name = "NUM_PREC_RADIX", columnType = IntType),
      mkColumn(name = "NULLABLE", columnType = IntType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "ATTR_DEF", columnType = StringType),
      mkColumn(name = "SQL_DATA_TYPE", columnType = IntType),
      mkColumn(name = "SQL_DATETIME_SUB", columnType = IntType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = IntType),
      mkColumn(name = "ORDINAL_POSITION", columnType = IntType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType),
      mkColumn(name = "SCOPE_CATALOG", columnType = StringType),
      mkColumn(name = "SCOPE_SCHEMA", columnType = StringType),
      mkColumn(name = "SCOPE_TABLE", columnType = StringType),
      mkColumn(name = "SOURCE_DATA_TYPE", columnType = IntType),
      mkColumn(name = "IS_AUTOINCREMENT", columnType = StringType),
      mkColumn(name = "IS_GENERATEDCOLUMN", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Attributes", columns, data = Nil)
  }

  override def getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet = {
    val columns = Seq(
      mkColumn(name = "SCOPE", columnType = IntType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType),
      mkColumn(name = "BUFFER_LENGTH", columnType = IntType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType),
      mkColumn(name = "PSEUDO_COLUMN", columnType = IntType))
    new JDBCResultSet(connection, catalog, schema, tableName = "BestRowIdentifiers", columns, data = Seq(
      Seq(1, ROWID_NAME, IntType.getJDBCType, "IntType", INT_BYTES, null, 0, 1).map(Option.apply)
    ))
  }

  override def getCatalogs: ResultSet = {
    val columns = Seq(mkColumn(name = "TABLE_CAT", columnType = StringType))
    val databases = connection.client.searchDatabases()
    new JDBCResultSet(connection, connection.getCatalog, connection.getSchema, tableName = "Catalogs", columns, data = databases.map { db =>
      Seq(Option(db.databaseName))
    })
  }

  override def getClientInfoProperties: ResultSet = {
    val columns = Seq(
      mkColumn(name = "NAME", columnType = StringType),
      mkColumn(name = "MAX_LEN", columnType = IntType),
      mkColumn(name = "DEFAULT_VALUE", columnType = StringType),
      mkColumn(name = "DESCRIPTION", columnType = StringType))
    new JDBCResultSet(connection, connection.getCatalog, connection.getSchema, tableName = "ClientInfoProperties", columns, data = Nil)
  }
  
  override def getColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType),
      mkColumn(name = "BUFFER_LENGTH", columnType = StringType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType),
      mkColumn(name = "NUM_PREC_RADIX", columnType = IntType),
      mkColumn(name = "NULLABLE", columnType = IntType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "COLUMN_DEF", columnType = StringType),
      mkColumn(name = "SQL_DATA_TYPE", columnType = IntType),
      mkColumn(name = "SQL_DATETIME_SUB", columnType = IntType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = IntType),
      mkColumn(name = "ORDINAL_POSITION", columnType = IntType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType),
      mkColumn(name = "SCOPE_CATALOG", columnType = StringType),
      mkColumn(name = "SCOPE_SCHEMA", columnType = StringType),
      mkColumn(name = "SCOPE_TABLE", columnType = StringType),
      mkColumn(name = "SOURCE_DATA_TYPE", columnType = IntType),
      mkColumn(name = "IS_AUTOINCREMENT", columnType = StringType),
      mkColumn(name = "IS_GENERATEDCOLUMN", columnType = StringType))
    val results = connection.client.searchColumns(databaseNamePattern = Some(catalog), tableNamePattern = Some(tableNamePattern), columnNamePattern = Some(columnNamePattern))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Columns", columns, data = results map { ti =>
      Seq(ti.databaseName, ti.databaseName, ti.tableName, ti.column.name, ti.column.metadata.`type`.getJDBCType,
        ti.column.metadata.`type`.toString, ti.column.sizeInBytes, 0, 0, 10, 0, ti.column.comment, "", null, null, ti.column.sizeInBytes,
        results.indexOf(ti), "YES",  null, null, null, 0, "NO", "NO").map(Option.apply)
    })
  }

  override def getColumnPrivileges(catalog: String, schema: String, table: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "GRANTOR", columnType = StringType),
      mkColumn(name = "GRANTEE", columnType = StringType),
      mkColumn(name = "PRIVILEGE", columnType = StringType),
      mkColumn(name = "IS_GRANTABLE", columnType = StringType))
    val results = connection.client.searchColumns(databaseNamePattern = Some(catalog), tableNamePattern = Some(table), columnNamePattern = Some(columnNamePattern))
    new JDBCResultSet(connection, catalog, schema, tableName = "ColumnPrivileges", columns, data = results map { ti =>
      Seq(ti.databaseName, ti.databaseName, ti.tableName, ti.column.name, "System", "Everyone", rights.mkString(","), "NO").map(Option(_))
    })
  }

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = IntType),
      mkColumn(name = "UPDATE_RULE", columnType = IntType),
      mkColumn(name = "DELETE_RULE", columnType = IntType),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = IntType))
    new JDBCResultSet(connection, parentCatalog, parentSchema, tableName = "CrossReferences", columns, data = Nil)
  }

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = IntType),
      mkColumn(name = "UPDATE_RULE", columnType = IntType),
      mkColumn(name = "DELETE_RULE", columnType = IntType),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = IntType))
    new JDBCResultSet(connection, catalog, schema, tableName = "ExportedKeys", columns, data = Nil)
  }

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "FUNCTION_CAT", columnType = StringType),
      mkColumn(name = "FUNCTION_SCHEM", columnType = StringType),
      mkColumn(name = "FUNCTION_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_TYPE", columnType = IntType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "PRECISION", columnType = IntType),
      mkColumn(name = "LENGTH", columnType = IntType),
      mkColumn(name = "SCALE", columnType = IntType),
      mkColumn(name = "RADIX", columnType = IntType),
      mkColumn(name = "NULLABLE", columnType = IntType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = IntType),
      mkColumn(name = "ORDINAL_POSITION", columnType = IntType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType),
      mkColumn(name = "SPECIFIC_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "FunctionColumns", columns, data = Nil)
  }

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "FUNCTION_CAT", columnType = StringType),
      mkColumn(name = "FUNCTION_SCHEM", columnType = StringType),
      mkColumn(name = "FUNCTION_NAME", columnType = StringType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "FUNCTION_TYPE", columnType = IntType),
      mkColumn(name = "SPECIFIC_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Functions", columns, data = Nil)
  }

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PKTABLE_CAT", columnType = StringType),
      mkColumn(name = "PKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "PKTABLE_NAME", columnType = StringType),
      mkColumn(name = "PKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "FKTABLE_CAT", columnType = StringType),
      mkColumn(name = "FKTABLE_SCHEM", columnType = StringType),
      mkColumn(name = "FKTABLE_NAME", columnType = StringType),
      mkColumn(name = "FKCOLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = IntType),
      mkColumn(name = "UPDATE_RULE", columnType = IntType),
      mkColumn(name = "DELETE_RULE", columnType = IntType),
      mkColumn(name = "FK_NAME", columnType = StringType),
      mkColumn(name = "PK_NAME", columnType = StringType),
      mkColumn(name = "DEFERRABILITY", columnType = IntType))
    new JDBCResultSet(connection, catalog, schema, tableName = "ImportedKeys", columns, data = Nil)
  }

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "NON_UNIQUE", columnType = BooleanType),
      mkColumn(name = "INDEX_QUALIFIER", columnType = BooleanType),
      mkColumn(name = "TYPE", columnType = StringType),
      mkColumn(name = "ORDINAL_POSITION", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "ASC_OR_DESC", columnType = StringType),
      mkColumn(name = "CARDINALITY", columnType = LongType),
      mkColumn(name = "PAGES", columnType = LongType),
      mkColumn(name = "FILTER_CONDITION", columnType = StringType))
    new JDBCResultSet(connection, catalog, schema, tableName = "Indices", columns, data = Nil)
  }

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PROCEDURE_CAT", columnType = StringType),
      mkColumn(name = "PROCEDURE_SCHEM", columnType = StringType),
      mkColumn(name = "PROCEDURE_NAME", columnType = StringType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "PROCEDURE_TYPE", columnType = StringType),
      mkColumn(name = "SPECIFIC_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Procedures", columns, data = Nil)
  }

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "PROCEDURE_CAT", columnType = StringType),
      mkColumn(name = "PROCEDURE_SCHEM", columnType = StringType),
      mkColumn(name = "PROCEDURE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_TYPE", columnType = IntType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "PRECISION", columnType = IntType),
      mkColumn(name = "LENGTH", columnType = IntType),
      mkColumn(name = "SCALE", columnType = IntType),
      mkColumn(name = "RADIX", columnType = IntType),
      mkColumn(name = "NULLABLE", columnType = IntType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "COLUMN_DEF", columnType = StringType),
      mkColumn(name = "SQL_DATA_TYPE", columnType = IntType),
      mkColumn(name = "SQL_DATETIME_SUB", columnType = IntType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = IntType),
      mkColumn(name = "ORDINAL_POSITION", columnType = IntType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType),
      mkColumn(name = "SPECIFIC_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "ProcedureColumns", columns, data = Nil)
  }

  override def getSchemas: ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_CATALOG", columnType = StringType))
    new JDBCResultSet(connection, connection.getCatalog, connection.getSchema, tableName = "Schemas", columns, data = Nil)
  }

  override def getTables(catalog: String, schemaPattern: String, tableNamePattern: String, types: Array[String]): ResultSet = {
    val types_? = Option(types)
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "TABLE_TYPE", columnType = StringType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "TYPE_CAT", columnType = StringType),
      mkColumn(name = "TYPE_SCHEM", columnType = StringType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "SELF_REFERENCING_COL_NAME", columnType = StringType),
      mkColumn(name = "REF_GENERATION", columnType = StringType))
    val summary = connection.client.getDatabaseSummary(catalog)
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Tables", columns, data = summary.tables flatMap { table =>
      if (types_?.isEmpty || types_?.exists(_.contains(table.tableType)))
        Some(Seq(catalog, connection.getSchema, table.tableName, table.tableType, table.description.orNull, null, null, null, null, null).map(Option.apply))
      else None
    })
  }

  override def getTypeInfo: ResultSet = {
    val columns = Seq(
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "PRECISION", columnType = IntType),
      mkColumn(name = "LITERAL_PREFIX", columnType = StringType),
      mkColumn(name = "LITERAL_SUFFIX", columnType = StringType),
      mkColumn(name = "CREATE_PARAMS", columnType = StringType),
      mkColumn(name = "NULLABLE", columnType = IntType))
    new JDBCResultSet(connection, connection.getCatalog, connection.getSchema, tableName = "Types", columns, data = values.toSeq.map { columnType =>
      Seq(columnType.toString, columnType.getJDBCType, columnType.getFixedLength.getOrElse(255), null, null, null, ResultSetMetaData.columnNullable).map(Option(_))
    })
  }

  override def getTableTypes: ResultSet = {
    val columns = Seq(mkColumn(name = "TABLE_TYPE", columnType = StringType))
    val tableTypes = DatabaseManagementSystem.tableTypes
    new JDBCResultSet(connection, connection.getCatalog, connection.getSchema, tableName = "TableTypes", columns, data = tableTypes map { tableType =>
      Seq(Option(tableType))
    })
  }

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "GRANTOR", columnType = StringType),
      mkColumn(name = "GRANTEE", columnType = StringType),
      mkColumn(name = "PRIVILEGE", columnType = StringType),
      mkColumn(name = "IS_GRANTABLE", columnType = StringType))
    val summary = connection.client.getDatabaseSummary(catalog)
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "TablePrivileges", columns, data = summary.tables map { tableName =>
      Seq(catalog, tableName, tableName, "System", "Everyone", rights.mkString(","), "NO").map(Option(_))
    })
  }

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "KEY_SEQ", columnType = IntType),
      mkColumn(name = "PK_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, schema, tableName = "PrimaryKeys", columns, data = Seq(
      Seq(catalog, schema, table, ROWID_NAME, 1, ROWID_NAME).map(Option.apply)
    ))
  }

  override def getPseudoColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType),
      mkColumn(name = "NUM_PREC_RADIX", columnType = StringType),
      mkColumn(name = "COLUMN_USAGE", columnType = StringType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = StringType),
      mkColumn(name = "IS_NULLABLE", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "PseudoColumns", columns, data = Nil)
  }

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "SUPERTYPE_CAT", columnType = StringType),
      mkColumn(name = "SUPERTYPE_SCHEM", columnType = StringType),
      mkColumn(name = "SUPERTABLE_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "SuperTypes", columns, data = Nil)
  }

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "SUPERTABLE_NAME", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "SuperTables", columns, data = Nil)
  }

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_CATALOG", columnType = StringType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "Schemas", columns, data = Nil)
  }

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType),
      mkColumn(name = "TABLE_NAME", columnType = StringType),
      mkColumn(name = "CLASS_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "REMARKS", columnType = StringType),
      mkColumn(name = "BASE_TYPE", columnType = IntType))
    new JDBCResultSet(connection, catalog, connection.getSchema, tableName = "UDTs", columns, data = Nil)
  }

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "SCOPE", columnType = IntType),
      mkColumn(name = "COLUMN_NAME", columnType = StringType),
      mkColumn(name = "DATA_TYPE", columnType = IntType),
      mkColumn(name = "TYPE_NAME", columnType = StringType),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType),
      mkColumn(name = "BUFFER_LENGTH", columnType = StringType),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType),
      mkColumn(name = "PSEUDO_COLUMN", columnType = IntType))
    new JDBCResultSet(connection, catalog, schema, tableName = "VersionColumns", columns, data = Nil)
  }

  private def mkColumn(name: String, columnType: ColumnType, sizeInBytes: Int = 256) = {
    Column(name = name, metadata = ColumnMetadata(`type` = columnType), comment = "", enumValues = Nil,
      sizeInBytes = columnType.getFixedLength.getOrElse(sizeInBytes))
  }

}

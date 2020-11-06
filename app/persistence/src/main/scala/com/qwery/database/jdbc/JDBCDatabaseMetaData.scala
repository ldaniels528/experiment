package com.qwery.database
package jdbc

import java.sql.{DatabaseMetaData, ResultSet, ResultSetMetaData, RowIdLifetime}

import com.qwery.database.ColumnTypes._
import com.qwery.database.models.TableColumn

import scala.beans.{BeanProperty, BooleanBeanProperty}

/**
 * Qwery Database Metadata
 * @param connection   the [[JDBCConnection connection]]
 * @param URL          the URL
 * @param databaseName the database name
 */
class JDBCDatabaseMetaData(@BeanProperty val connection: JDBCConnection,
                           @BeanProperty val URL: String,
                           databaseName: String)
  extends DatabaseMetaData with JDBCWrapper {

  @BeanProperty val catalogSeparator: String = "."
  @BeanProperty val catalogTerm: String = "DATABASE"
  @BeanProperty val databaseMajorVersion: Int = 0
  @BeanProperty val databaseMinorVersion: Int = 1
  @BeanProperty val databaseProductName: String = "Qwery"
  @BeanProperty val databaseProductVersion: String = s"$databaseMajorVersion.$databaseMinorVersion"
  @BeanProperty val defaultTransactionIsolation: Int = ResultSet.CONCUR_UPDATABLE
  @BeanProperty val driverMajorVersion: Int = 0
  @BeanProperty val driverMinorVersion: Int = 1
  @BeanProperty val driverVersion: String = s"$driverMajorVersion.$driverMinorVersion"
  @BeanProperty val driverName: String = s"Qwery v$driverVersion"
  @BeanProperty val extraNameCharacters: String = ""
  @BeanProperty val numericFunctions: String =
    """|ABS
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
    """|LEN TRIM
       |""".stripMargin.trim.replaceAllLiterally("\n", " ")
  @BeanProperty val systemFunctions: String =
    """|NOW
       |""".stripMargin.trim.replaceAllLiterally("\n", " ")
  @BeanProperty val SQLKeywords: String =
    """|AS BETWEEN ON SELECT UPDATE
       |""".stripMargin.trim.replaceAllLiterally("\n", " ")
  @BeanProperty var SQLStateType: Int = _
  @BeanProperty val timeDateFunctions: String =
    """|NOW
       |""".stripMargin.trim.replaceAllLiterally("\n", " ")
  @BeanProperty val userName: String = ""

  override def allProceduresAreCallable(): Boolean = true

  override def allTablesAreSelectable(): Boolean = true

  override def generatedKeyAlwaysReturned(): Boolean = true

  override def nullsAreSortedHigh(): Boolean = true

  override def nullsAreSortedLow(): Boolean = !nullsAreSortedHigh()

  override def nullsAreSortedAtStart(): Boolean = false

  override def nullsAreSortedAtEnd(): Boolean = !nullsAreSortedAtStart()

  override def usesLocalFiles(): Boolean = false

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
    new JDBCResultSet(connection, databaseName, tableName = "Attributes", columns = Nil, data = Nil)
  }

  override def getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet = {
    val columns = Seq(
      mkColumn(name = "SCOPE", columnType = ShortType, comment = Some("actual scope of result")),
      mkColumn(name = "COLUMN_NAME", columnType = StringType, comment = Some("column name")),
      mkColumn(name = "DATA_TYPE", columnType = IntType, comment = Some("SQL data type from java.sql.Types")),
      mkColumn(name = "TYPE_NAME", columnType = StringType, comment = Some("Data source dependent type name, for a UDT the type name is fully qualified")),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType, comment = Some("precision")),
      mkColumn(name = "BUFFER_LENGTH", columnType = IntType, comment = Some("not used")),
      mkColumn(name = "DECIMAL_DIGITS", columnType = ShortType, comment = Some("scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.")),
      mkColumn(name = "PSEUDO_COLUMN", columnType = ShortType, comment = Some("is this a pseudo column like an Oracle ROWID")))
    new JDBCResultSet(connection, databaseName, tableName = "BestRowIdentifier", columns = columns, data = Nil)
  }

  override def getCatalogs: ResultSet = {
    val columns = Seq(mkColumn(name = "TABLE_CAT", columnType = StringType, comment = Some("catalog name")))
    new JDBCResultSet(connection, databaseName, tableName = "Catalogs", columns = columns, data = Nil)
  }

  override def getClientInfoProperties: ResultSet = {
    val columns = Seq(
      mkColumn(name = "NAME", columnType = StringType, comment = Some("The name of the client info property")),
      mkColumn(name = "MAX_LEN", columnType = IntType, comment = Some("The maximum length of the value for the property")),
      mkColumn(name = "DEFAULT_VALUE", columnType = StringType, comment = Some("The default value of the property")),
      mkColumn(name = "DESCRIPTION", columnType = StringType, comment = Some("A description of the property. This will typically contain information as to where this property is stored in the database.")))
    new JDBCResultSet(connection, databaseName, tableName = "ClientInfo", columns = columns, data = Nil)
  }

  override def getColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "Columns", columns = Nil, data = Nil)
  }

  override def getColumnPrivileges(catalog: String, schema: String, table: String, columnNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "ColumnPrivileges", columns = Nil, data = Nil)
  }

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "CrossReference", columns = Nil, data = Nil)
  }

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "ExportedKeys", columns = Nil, data = Nil)
  }

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "FunctionColumns", columns = Nil, data = Nil)
  }

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "Functions", columns = Nil, data = Nil)
  }

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "ImportedKeys", columns = Nil, data = Nil)
  }

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType, comment = Some("table catalog (may be null)")),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType, comment = Some("table schema (may be null)")),
      mkColumn(name = "TABLE_NAME", columnType = StringType, comment = Some("table name")),
      mkColumn(name = "NON_UNIQUE", columnType = BooleanType, comment = Some("Can index values be non-unique. false when TYPE is tableIndexStatistic")),
      mkColumn(name = "INDEX_QUALIFIER", columnType = BooleanType, comment = Some("index catalog (may be null); null when TYPE is tableIndexStatistic")),
      mkColumn(name = "TYPE", columnType = StringType, comment = Some("index type")),
      mkColumn(name = "ORDINAL_POSITION", columnType = StringType, comment = Some("column sequence number within index; zero when TYPE is tableIndexStatistic")),
      mkColumn(name = "COLUMN_NAME", columnType = StringType, comment = Some("column name; null when TYPE is tableIndexStatistic")),
      mkColumn(name = "ASC_OR_DESC", columnType = StringType, comment = Some("column sort sequence, \"A\" => ascending, \"D\" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic")),
      mkColumn(name = "CARDINALITY", columnType = LongType, comment = Some("When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.")),
      mkColumn(name = "PAGES", columnType = LongType, comment = Some("When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.")),
      mkColumn(name = "FILTER_CONDITION", columnType = StringType, comment = Some("Filter condition, if any. (may be null)")))
    new JDBCResultSet(connection, databaseName, tableName = "Indices", columns = columns, data = Nil)
  }

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "Procedures", columns = Nil, data = Nil)
  }

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "ProcedureColumns", columns = Nil, data = Nil)
  }

  override def getSchemas: ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_SCHEM", columnType = StringType, comment = Some("schema name")),
      mkColumn(name = "TABLE_CATALOG", columnType = StringType, comment = Some("catalog name (may be null)")))
    new JDBCResultSet(connection, databaseName, tableName = "Schemas", columns = columns, data = Nil)
  }

  override def getTables(catalog: String, schemaPattern: String, tableNamePattern: String, types: Array[String]): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType, comment = Some("table catalog")),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType, comment = Some("table schema")),
      mkColumn(name = "TABLE_NAME", columnType = StringType, comment = Some("table name")),
      mkColumn(name = "TABLE_TYPE", columnType = StringType, comment = Some("table type")),
      mkColumn(name = "REMARKS", columnType = StringType, comment = Some("explanatory comment on the table")),
      mkColumn(name = "TYPE_CAT", columnType = StringType, comment = Some("the types catalog")),
      mkColumn(name = "TYPE_SCHEM", columnType = StringType, comment = Some("the types schema")),
      mkColumn(name = "TYPE_NAME", columnType = StringType, comment = Some("type name")),
      mkColumn(name = "SELF_REFERENCING_COL_NAME", columnType = StringType, comment = Some("name of the designated \"identifier\" column of a typed table")),
      mkColumn(name = "REF_GENERATION", columnType = StringType, comment = Some("specifies how values in SELF_REFERENCING_COL_NAME are created.")))
    val metrics = connection.service.getDatabaseMetrics(catalog)
    new JDBCResultSet(connection, databaseName, tableName = "Tables", columns = columns, data = metrics.tables map { tableName =>
      Seq(databaseName, databaseName, tableName, "TABLE", "", null, null, "TABLE", null, null).map(Option(_))
    })
  }

  override def getTypeInfo: ResultSet = {
    val columns = Seq(
      mkColumn(name = "TYPE_NAME", columnType = StringType, comment = Some("Type name")),
      mkColumn(name = "DATA_TYPE", columnType = IntType, comment = Some("SQL data type from java.sql.Types")),
      mkColumn(name = "PRECISION", columnType = IntType, comment = Some("maximum precision")),
      mkColumn(name = "LITERAL_PREFIX", columnType = StringType, comment = Some("prefix used to quote a literal (may be null)")),
      mkColumn(name = "LITERAL_SUFFIX", columnType = StringType, comment = Some("suffix used to quote a literal (may be null)")),
      mkColumn(name = "CREATE_PARAMS", columnType = StringType, comment = Some("parameters used in creating the type (may be null)")),
      mkColumn(name = "NULLABLE", columnType = ShortType, comment = Some("can you use NULL for this type.")))
    new JDBCResultSet(connection, databaseName, tableName = "Types", columns, data = values.toSeq.map { columnType =>
      Seq(columnType.toString, columnType.getJDBCType(), columnType.getFixedLength.getOrElse(255), null, null, null, ResultSetMetaData.columnNullable).map(Option(_))
    })
  }

  override def getTableTypes: ResultSet = {
    val columns = Seq(mkColumn(name = "TABLE_CAT", columnType = StringType, comment = Some("catalog name")))
    val tableTypes = Seq("TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM")
    new JDBCResultSet(connection, databaseName, tableName = "TableTypes", columns = columns, data = tableTypes map { tableType =>
      Seq(Option(tableType))
    })
  }

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "TablePrivileges", columns = Nil, data = Nil)
  }

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "VersionColumns", columns = Nil, data = Nil)
  }

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "BestRowIdentifier", columns = Nil, data = Nil)
  }

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "SuperTypes", columns = Nil, data = Nil)
  }

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "SuperTables", columns = Nil, data = Nil)
  }

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "Schemas", columns = Nil, data = Nil)
  }

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet = {
    new JDBCResultSet(connection, databaseName, tableName = "UDTs", columns = Nil, data = Nil)
  }

  override def getPseudoColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = {
    val columns = Seq(
      mkColumn(name = "TABLE_CAT", columnType = StringType, comment = Some("table catalog")),
      mkColumn(name = "TABLE_SCHEM", columnType = StringType, comment = Some("table schema")),
      mkColumn(name = "TABLE_NAME", columnType = StringType, comment = Some("table name")),
      mkColumn(name = "COLUMN_NAME", columnType = StringType, comment = Some("column name")),
      mkColumn(name = "DATA_TYPE", columnType = IntType, comment = Some("SQL data type from java.sql.Types")),
      mkColumn(name = "COLUMN_SIZE", columnType = IntType, comment = Some("column size.")),
      mkColumn(name = "DECIMAL_DIGITS", columnType = IntType, comment = Some("the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.")),
      mkColumn(name = "NUM_PREC_RADIX", columnType = StringType, comment = Some("Radix (typically either 10 or 2)")),
      mkColumn(name = "COLUMN_USAGE", columnType = StringType, comment = Some("The allowed usage for the column. The value returned will correspond to the enum name returned by PseudoColumnUsage.name()")),
      mkColumn(name = "REMARKS", columnType = StringType, comment = Some("comment describing column (may be null)")),
      mkColumn(name = "CHAR_OCTET_LENGTH", columnType = StringType, comment = Some("for char types the maximum number of bytes in the column")),
      mkColumn(name = "IS_NULLABLE", columnType = StringType, comment = Some("ISO rules are used to determine the nullability for a column.")))
    new JDBCResultSet(connection, databaseName, tableName = "PseudoColumns", columns = columns, data = Nil)
  }

  private def mkColumn(name: String, columnType: ColumnType, sizeInBytes: Int = 256, comment: Option[String] = None) = {
    TableColumn(name = name, columnType = columnType.toString, comment = comment, sizeInBytes = columnType.getFixedLength.getOrElse(sizeInBytes))
  }

}

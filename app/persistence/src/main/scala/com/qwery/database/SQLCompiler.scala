package com.qwery.database

import com.qwery.database.QueryProcessor.commands.DatabaseIORequest
import com.qwery.database.QueryProcessor.{commands => cx}
import com.qwery.database.SQLCompiler.implicits.InvokableFacade
import com.qwery.database.models.TableColumn.ColumnToTableColumnConversion
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models.expressions.{Condition, ConditionalOp, Expression, Literal}
import com.qwery.models.{Invokable, TableIndex, TableRef, expressions => ex}
import com.qwery.{models => mx}

/**
 * SQL Compiler - compiles SQL into [[DatabaseIORequest]]s
 */
object SQLCompiler {
  private val columnTypeMap = Map(
    "ARRAY" -> ColumnTypes.ArrayType,
    "BIGINT" -> ColumnTypes.BigIntType,
    "BINARY" -> ColumnTypes.BinaryType,
    "BLOB" -> ColumnTypes.BlobType,
    "BOOLEAN" -> ColumnTypes.BooleanType,
    "CHAR" -> ColumnTypes.StringType,
    "CLOB" -> ColumnTypes.ClobType,
    "DATE" -> ColumnTypes.DateType,
    "DATETIME" -> ColumnTypes.DateType,
    "DECIMAL" -> ColumnTypes.BigDecimalType,
    "DOUBLE" -> ColumnTypes.DoubleType,
    "FLOAT" -> ColumnTypes.FloatType,
    "INT" -> ColumnTypes.IntType,
    "INTEGER" -> ColumnTypes.IntType,
    "LONG" -> ColumnTypes.LongType,
    "OBJECT" -> ColumnTypes.SerializableType,
    "REAL" -> ColumnTypes.DoubleType,
    "SHORT" -> ColumnTypes.ShortType,
    "SMALLINT" -> ColumnTypes.ShortType,
    "STRING" -> ColumnTypes.StringType,
    "TEXT" -> ColumnTypes.ClobType,
    "TIMESTAMP" -> ColumnTypes.DateType,
    "TINYINT" -> ColumnTypes.ByteType,
    "UUID" -> ColumnTypes.UUIDType,
    "VARCHAR" -> ColumnTypes.StringType
  )

  def compile(databaseName: String, sql: String): DatabaseIORequest = {
    val model = SQLLanguageParser.parse(sql)
    model.compile(databaseName)
  }

  /**
   * Implicit classes and conversions
   */
  object implicits {

    /**
     * Expression Facade
     * @param expression the [[Expression]]
     */
    final implicit class ExpressionFacade(val expression: Expression) extends AnyVal {
      def translate: Any = expression match {
        case Literal(value) => value
        case unknown => die(s"Unsupported value $unknown")
      }
    }

    /**
     * Invokable Facade
     * @param invokable the [[Invokable]]
     */
    final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {

      def compile(databaseName: String): DatabaseIORequest = invokable match {
        case mx.Create(table: mx.Table) =>
          cx.CreateTable(databaseName, table.name, columns = table.columns.map(_.toColumn.toTableColumn))
        case mx.Create(TableIndex(indexName, TableRef(tableName), columns)) =>
          cx.CreateIndex(databaseName, tableName, indexName, indexColumnName = columns.map(_.name).onlyOne())
        case mx.Delete(TableRef(tableName), where, limit) =>
          cx.DeleteRows(databaseName, tableName, condition = toCriteria(where), limit)
        case mx.DropTable(TableRef(tableName), ifExists) =>
          cx.DropTable(databaseName, tableName, ifExists)
        case mx.Insert(Into(TableRef(tableName)), mx.Insert.Values(expressionValues), fields) =>
          cx.InsertRows(databaseName, tableName, columns = fields.map(_.name), values = expressionValues.map(_.map(_.translate)))
        case mx.Insert(Overwrite(TableRef(tableName)), mx.Insert.Values(expressionValues), fields) =>
          cx.InsertRows(databaseName, tableName, columns = fields.map(_.name), values = expressionValues.map(_.map(_.translate)))
        case mx.Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
          cx.SelectRows(databaseName, tableName, fields, toCriteria(where), limit)
        case mx.Truncate(TableRef(tableName)) =>
          cx.TruncateTable(databaseName, tableName)
        case mx.Update(TableRef(tableName), changes, where, limit) =>
          cx.UpdateRows(databaseName, tableName, changes = TupleSet(changes.map { case (k, v) => k -> v.translate }: _*), condition = toCriteria(where), limit)
        case unknown => die(s"Unsupported operation $unknown")
      }

      private def toCriteria(condition_? : Option[Condition]): TupleSet = condition_? match {
        case Some(ConditionalOp(ex.Field(name), value, "==", "=")) => TupleSet(name -> value.translate)
        case Some(condition) => die(s"Unsupported condition $condition")
        case None => TupleSet()
      }

    }

    /**
     * Item Sequence Utilities
     * @param items the collection of items
     * @tparam A the item type
     */
    final implicit class ItemSeqUtilities[A](val items: Seq[A]) extends AnyVal {
      @inline
      def onlyOne(label: => String = "column"): A = items.toList match {
        case value :: Nil => value
        case _ => die(s"Multiple ${label}s are not supported")
      }
    }

    /**
     * SQL Column-To-Column Conversion
     * @param column the [[mx.Column SQL Column]]
     */
    final implicit class SQLToColumnConversion(val column: mx.Column) extends AnyVal {
      @inline
      def toColumn: Column = Column(
        name = column.name,
        comment = column.comment.getOrElse(""),
        enumValues = column.enumValues,
        maxSize = column.spec.precision.headOption,
        metadata = ColumnMetadata(
          isNullable = column.isNullable,
          `type` = columnTypeMap.getOrElse(column.spec.typeName, ColumnTypes.BlobType)
        ))
    }

  }

}

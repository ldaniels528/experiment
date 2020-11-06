package com.qwery.database

import com.qwery.database.QueryProcessor.commands.{DatabaseIORequest, FindRows}
import com.qwery.database.QueryProcessor.{commands => cx}
import com.qwery.database.models.TableColumn.ColumnToTableColumnConversion
import com.qwery.models.Insert.Into
import com.qwery.models.expressions.{Condition, ConditionalOp, Expression, Literal}
import com.qwery.models.{Invokable, TableIndex, TableRef, TypeAsEnum, expressions => ex}
import com.qwery.{models => mx}

/**
 * SQL Model Cross-Compiler - translates [[Invokable]]s into [[DatabaseIORequest]]s
 */
object SQLModelCrossCompiler {
  private val columnTypeMap = Map(
    "ARRAY" -> ColumnTypes.ArrayType,
    "BINARY" -> ColumnTypes.BlobType,
    "BOOLEAN" -> ColumnTypes.BooleanType,
    "DATE" -> ColumnTypes.DateType,
    "DOUBLE" -> ColumnTypes.DoubleType,
    "FLOAT" -> ColumnTypes.FloatType,
    "INTEGER" -> ColumnTypes.IntType,
    "LONG" -> ColumnTypes.LongType,
    "SHORT" -> ColumnTypes.ShortType,
    "STRING" -> ColumnTypes.StringType,
    "TIMESTAMP" -> ColumnTypes.DateType,
    "UUID" -> ColumnTypes.UUIDType
  )

  /**
   * Implicit classes and conversions
   */
  object implicits {

    final implicit class ExpressionFacade(val expression: Expression) extends AnyVal {
      def translate: Any = expression match {
        case Literal(value) => value
        case unknown => die(s"Unsupported value $unknown")
      }
    }

    final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
      def extractTableName: String = invokable match {
        case mx.Create(table: mx.Table) => table.name
        case mx.Create(TypeAsEnum(name, _)) => name
        case mx.Delete(TableRef(tableName), _, _) => tableName
        case mx.DropTable(TableRef(tableName), _) => tableName
        case mx.Insert(Into(TableRef(tableName)), _, _) => tableName
        case select: mx.Select => select.from.map(_.extractTableName).getOrElse(die(s"No table reference found in $select"))
        case mx.TableRef(tableName) => tableName
        case mx.Truncate(TableRef(tableName)) => tableName
        case mx.Update(TableRef(tableName), _, _, _) => tableName
        case unknown => die(s"Unsupported operation $unknown")
      }

      def compile(databaseName: String): DatabaseIORequest = invokable match {
        case mx.Create(table: mx.Table) =>
          cx.CreateTable(databaseName, table.name, columns = table.columns.map(_.toColumn.toTableColumn))
        case mx.Create(TableIndex(indexName, TableRef(tableName), columns)) =>
          cx.CreateIndex(databaseName, tableName, indexName, columns.map(_.name).onlyOne("Multiple columns is not supported"))
        case mx.Delete(TableRef(tableName), where, limit) =>
          cx.DeleteRows(databaseName, tableName, condition = toCriteria(where), limit)
        case mx.DropTable(TableRef(tableName), ifExists) =>
          cx.DropTable(databaseName, tableName, ifExists)
        case mx.Insert(Into(TableRef(tableName)), mx.Insert.Values(expressionValues), fields) =>
          cx.InsertRows(databaseName, tableName, columns = fields.map(_.name), values = expressionValues.map(_.map(_.translate)))
        case select: mx.Select =>
          toSelectRows(databaseName, select)
        case mx.Truncate(TableRef(tableName)) =>
          cx.TruncateTable(databaseName, tableName)
        case mx.Update(TableRef(tableName), assignments, where, limit) =>
          cx.UpdateRows(databaseName, tableName, values = TupleSet(assignments.map { case (k, v) => k -> v.translate }:_*), condition = toCriteria(where), limit)
        case unknown => die(s"Unsupported operation $unknown")
      }

      private def toCriteria(condition_? : Option[Condition]): TupleSet = condition_? match {
        case Some(ConditionalOp(ex.Field(name), value, "==", "=")) => TupleSet(name -> value.translate)
        case Some(condition) => die(s"Unsupported condition $condition")
        case None => TupleSet()
      }

      private def toSelectRows(databaseName: String, select: mx.Select): DatabaseIORequest = {
        select.from match {
          case Some(TableRef(tableName)) =>
            FindRows(databaseName, tableName, toCriteria(select.where), limit = select.limit)
          case Some(queryable) => die(s"Unsupported queryable $queryable")
          case None => die("No query source was specified")
        }
      }

    }

    final implicit class ItemSeqUtilities[A](val items: Seq[A]) extends AnyVal {
      @inline
      def onlyOne(message: => String = "Only one identifier was expected"): A = items.toList match {
        case value :: Nil => value
        case _ => die(message)
      }
    }

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

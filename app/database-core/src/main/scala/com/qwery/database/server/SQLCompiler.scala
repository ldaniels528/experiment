package com.qwery.database
package server

import com.qwery.database.ExpressionVM._
import com.qwery.database.models.{Column, ColumnMetadata, ColumnTypes, KeyValues}
import com.qwery.database.server.QueryProcessor.commands.DatabaseIORequest
import com.qwery.database.server.QueryProcessor.{commands => cx}
import com.qwery.database.server.SQLCompiler.implicits.{ExpressionFacade, InvokableFacade}
import com.qwery.language.SQLLanguageParser
import com.qwery.language.SQLLanguageParser.implicits.ItemSeqUtilities
import com.qwery.models.Insert.{Into, Overwrite}
import com.qwery.models.expressions.{Condition, ConditionalOp, Expression, Literal}
import com.qwery.models.{EntityRef, Invokable, TableIndex, expressions => ex}
import com.qwery.{models => mx}

/**
 * SQL Compiler - compiles SQL into [[DatabaseIORequest]]s
 */
object SQLCompiler {

  def compile(databaseName: String, sql: String): DatabaseIORequest = {
    val model = SQLLanguageParser.parse(sql)
    model.compile(databaseName)
  }

  def toCriteria(condition_? : Option[Condition]): KeyValues = condition_? match {
    case Some(ConditionalOp(ex.FieldRef(name), value, "==", "=")) => KeyValues(name -> value.translate)
    case Some(condition) => die(s"Unsupported condition $condition")
    case None => KeyValues()
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

      def toColumn: Column = expression match {
        case ex.CurrentRow =>
          Column.create(name = "CurrentRow", metadata = ColumnMetadata(`type` = ColumnTypes.IntType), maxSize = Some(ROW_ID_BYTES))
        case expr =>
          Column.create(name = expr.alias getOrElse nextID, metadata = models.ColumnMetadata(`type` = ColumnTypes.DoubleType), maxSize = Some(LONG_BYTES))
      }
    }

    /**
     * Invokable Facade
     * @param invokable the [[Invokable]]
     */
    final implicit class InvokableFacade(val invokable: Invokable) extends AnyVal {
      def compile(databaseName: String): DatabaseIORequest = invokable match {
        case mx.Create(table: mx.Table) =>
          cx.CreateTable(databaseName, table.ref.name, table)
        case mx.Create(TableIndex(_, EntityRef(databaseName_?, schemaName_?, tableName), columns, ifNotExists)) =>
          cx.CreateIndex(databaseName, tableName, indexColumnName = columns.onlyOne())
        case mx.Create(mx.View(ref, invokable, description, ifNotExists)) =>
          cx.CreateView(databaseName, ref.name, description, invokable, ifNotExists)
        case mx.Delete(EntityRef(databaseName_?, schemaName_?, tableName), where, limit) =>
          cx.DeleteRows(databaseName, tableName, condition = toCriteria(where), limit)
        case mx.DropTable(EntityRef(databaseName_?, schemaName_?, tableName), ifExists) =>
          cx.DropTable(databaseName, tableName, ifExists)
        case mx.DropView(EntityRef(databaseName_?, schemaName_?, tableName), ifExists) =>
          cx.DropView(databaseName, tableName, ifExists)
        case mx.Insert(Into(EntityRef(databaseName_?, schemaName_?, tableName)), mx.Insert.Values(values), fields) =>
          cx.InsertRows(databaseName, tableName, columns = fields.map(_.name), values)
        case mx.Insert(Into(EntityRef(databaseName_?, schemaName_?, tableName)), queryable: mx.Queryable, fields) =>
          cx.InsertSelect(databaseName, tableName, queryable.compile(databaseName) match {
            case select: cx.SelectRows => select
            case other => die(s"Unhandled sub-command $other for INSERT INTO")
          })
        case mx.Insert(Overwrite(EntityRef(databaseName_?, schemaName_?, tableName)), mx.Insert.Values(values), fields) =>
          cx.InsertRows(databaseName, tableName, columns = fields.map(_.name), values)
        case mx.Insert(Overwrite(EntityRef(databaseName_?, schemaName_?, tableName)), select: mx.Select, fields) =>
          cx.InsertSelect(databaseName, tableName, select.compile(databaseName) match {
            case select: cx.SelectRows => select
            case other => die(s"Unhandled sub-command $other for INSERT OVERWRITE")
          })
        case mx.Select(fields, Some(EntityRef(databaseName_?, schemaName_?, tableName)), joins, groupBy, having, orderBy, where, limit) =>
          cx.SelectRows(databaseName, tableName, fields, toCriteria(where), groupBy, having, orderBy, limit)
        case mx.Truncate(EntityRef(databaseName_?, schemaName_?, tableName)) =>
          cx.TruncateTable(databaseName, tableName)
        case mx.Update(EntityRef(databaseName_?, schemaName_?, tableName), changes, where, limit) =>
          cx.UpdateRows(databaseName, tableName, changes = changes, condition = toCriteria(where), limit)
        case unknown => die(s"Unsupported operation $unknown")
      }

    }

  }

}

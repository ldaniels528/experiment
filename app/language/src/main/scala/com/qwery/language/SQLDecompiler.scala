package com.qwery
package language

import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models.AlterTable._
import com.qwery.models.JoinTypes.JoinType
import com.qwery.models._
import com.qwery.models.expressions.IntervalTypes.IntervalType
import com.qwery.models.expressions.Over.DataAccessTypes.DataAccessType
import com.qwery.models.expressions.Over.{Following, Preceding, Unbounded, WindowBetween}
import com.qwery.models.expressions._

/**
 * SQL Decompiler Support
 * @author lawrence.daniels@gmail.com
 */
trait SQLDecompiler {

  /**
   * Decompiles the opCode into the equivalent SQL statement
   * @param opCode the opCode
   * @return the equivalent SQL statement
   */
  def decompile(opCode: AnyRef): String = {
    val sql: String = opCode match {
      case a: AlterTable => decompileAlterTable(a)
      case c: Column => decompileColumn(c)
      case c: ColumnTypeSpec => decompileColumnTypeSpec(c)
      case c: Console => decompileConsole(c)
      case c: Create => decompileCreate(c)
      case d: DataAccessType => d.toString.toLowerCase
      case Declare(variable, _type) => s"declare ${variable.toSQL} ${_type}"
      case d: Delete => decompileDelete(d)
      case DoWhile(code, condition) => s"do ${code.toSQL} while ${condition.toSQL}"
      case e: EntityRef => decompileEntityRef(e)
      case Except(query0, query1) => s"${query0.toSQL} except ${query1.toSQL}"
      case e: Expression => decompileExpression(e)
      case f: ForEach => s"for ${f.variable.toSQL} in ${if (f.isReverse) "reverse" else ""} (${f.rows.toSQL}) ${f.invokable.toSQL}"
      case IN.Values(items) => items.map(_.toSQL).mkString(",")
      case Include(path) => s"include '$path'"
      case i: Insert => decompileInsert(i)
      case Insert.Values(items) => s"values ${items.map(lst => s"(${lst.map(_.toSQL).mkString(",")})").mkString(",")}"
      case Intersect(query0, query1) => s"${query0.toSQL} intersect ${query1.toSQL}"
      case i: IntervalType => i.toString.toLowerCase
      case j: Join => decompileJoin(j)
      case j: JoinType => s"${j.toString.toLowerCase.replaceAllLiterally("_", " ")} join"
      case OrderColumn(name, isAscending) => s"$name ${if (isAscending) "asc" else "desc"}"
      case ProcedureCall(name, args) => s"call $name(${args.map(_.toSQL).mkString(",")})"
      case Return(value) => value.map(v => s"return (${v.toSQL})") getOrElse "return"
      case RowSetVariableRef(name) => s"@$name"
      case s: Select => decompileSelect(s)
      case SetLocalVariable(name, expression) => s"set $$$name = ${expression.toSQL}"
      case SetRowVariable(name, dataset) => s"set @$name = (${dataset.toSQL})"
      case s: Show => decompileShow(s)
      case s: SQL => decompileSQL(s)
      case Truncate(table) => s"truncate ${table.toSQL}"
      case Union(query0, query1, isDistinct) => s"${query0.toSQL} union ${if (isDistinct) "distinct" else ""} ${query1.toSQL}"
      case u: Update => decompileUpdate(u)
      case WhileDo(condition, code) => s"while ${condition.toSQL} do ${code.toSQL}"
      case x => fail(x)
    }

    // does the expression have an alias?
    opCode match {
      case expr: Aliasable => sql.withAlias(expr.alias)
      case _ => sql
    }
  }

  private def decompileAlterTable(a: AlterTable): String = {
    val default: Column => String = _.defaultValue.map(v => s" default '$v'").orBlank
    s"alter table ${a.ref.toSQL} ${a.alterations map {
      case AddColumn(column) => s"add column ${column.toSQL}${default(column)}"
      case AppendColumn(column) => s"append column ${column.toSQL}${default(column)}"
      case DropColumn(columnName) => s"drop column $columnName"
      case PrependColumn(column) => s"prepend column ${column.toSQL}${default(column)}"
      case RenameColumn(oldName, newName) => s"rename $oldName as $newName"
      case x => fail(x)
    } mkString " "}"
  }

  private def decompileColumn(c: Column): String = {
    import c._
    val sb = new StringBuilder(s"$name ${spec.toSQL} ")
    if (enumValues.nonEmpty) sb.append(s"as enum (${enumValues.map(v => s"'$v'").mkString(",")}) ")
    if (!isNullable) sb.append("not null ")
    defaultValue.foreach(value => sb.append(s"default '$value' "))
    comment.foreach(remark => sb.append(s" comment '$remark' "))
    sb.toString()
  }

  private def decompileColumnTypeSpec(c: ColumnTypeSpec): String = {
    val sb = new StringBuilder(s"${c.`type`.toLowerCase}")
    if (c.size.nonEmpty || c.precision.nonEmpty)
      sb.append(s"(${(c.size.toList ::: c.precision.toList).mkString(",")})")
    sb.toString()
  }

  private def decompileConsole(c: Console): String = c match {
    case Console.Debug(text) => s"debug '$text'"
    case Console.Error(text) => s"error '$text'"
    case Console.Info(text) => s"info '$text'"
    case Console.Print(text) => s"print '$text'"
    case Console.Println(text) => s"println '$text'"
    case Console.Warn(text) => s"warn '$text'"
    case x => fail(x)
  }

  private def decompileCreate(c: Create): String = c match {
    case Create(e: ExternalTable) => decompileCreateExternalTable(e)
    case Create(p: Procedure) => decompileCreateProcedure(p)
    case Create(t: Table) => decompileCreateTable(t)
    case Create(t: TableIndex) => s"create index ${t.ref.toSQL} on ${t.table.toSQL} (${t.columns.mkString(",")})"
    case Create(t: TypeAsEnum) => s"create type ${t.ref.toSQL} as enum (${t.values.map(v => s"'$v'").mkString(",")})"
    case Create(u: UserDefinedFunction) => decompileCreateUserDefinedFunction(u)
    case Create(v: View) => decompileCreateView(v)
    case x => fail(x)
  }

  private def decompileCreateExternalTable(t: ExternalTable): String = {
    val sb = new StringBuilder(s"create external table ")
    if (t.ifNotExists) sb.append("if not exists ")

    // name, columns and partitioned by
    sb.append(t.ref.toSQL).append(" (").append(t.columns.map(_.toSQL).mkString(",")).append(") ")
    if (t.partitionBy.nonEmpty) sb.append(s"partitioned by (${t.partitionBy.map(_.toSQL).mkString(",")}) ")

    // rows and fields
    t.fieldTerminator.foreach(term => sb.append(s"fields terminated by '$term' "))
    t.lineTerminator.foreach(term => sb.append(s"row terminated by '$term' "))
    if (t.inputFormat.nonEmpty || t.outputFormat.nonEmpty) {
      sb.append("stored as ")
      t.inputFormat.foreach(format => sb.append(s"inputFormat '$format' "))
      t.outputFormat.foreach(format => sb.append(s"outputFormat '$format' "))
    }

    // location and tblProperties
    t.location.foreach(path => sb.append(s"location '$path' "))
    if (t.tableProperties.nonEmpty) {
      sb.append(s"tblProperties (${t.tableProperties.map { case (k, v) => s"'$k'='$v'" } mkString ","}) ")
    }

    // with clauses
    t.description.foreach(comment => sb.append(s"with comment '$comment' "))
    t.headersIncluded.foreach(enabled => if (enabled) sb.append("with headers on "))
    t.nullValue.foreach(value => sb.append(s"with null values as '$value' "))
    if (t.serdeProperties.nonEmpty) {
      sb.append(s"with serdeProperties (${t.serdeProperties.map { case (k, v) => s"'$k'='$v'" } mkString ","}) ")
    }
    sb.toString()
  }

  private def decompileCreateProcedure(p: Procedure): String = {
    val sb = new StringBuilder("create")
    if (p.isReplace) sb.append(" or replace")
    sb.append(s" procedure ${p.ref.toSQL}(${p.params.map(_.toSQL).mkString(",")}) as ${p.code.toSQL}")
    sb.toString()
  }

  private def decompileCreateTable(t: Table): String = {
    val sb = new StringBuilder(s"create table")
    if (t.ifNotExists) sb.append(" if not exists")
    sb.append(s" ${t.ref.toSQL} (${t.columns.map(_.toSQL).mkString(",")})")
    t.description.foreach(comment => sb.append(s" comment '$comment' "))
    sb.toString()
  }

  private def decompileCreateUserDefinedFunction(u: UserDefinedFunction): String = {
    val sb = new StringBuilder("create function")
    if (u.ifNotExists) sb.append(" if not exists")
    sb.append(s" ${u.ref.toSQL} as '${u.`class`}'")
    u.jarLocation.foreach(jar => sb.append(s" using jar '$jar'"))
    u.description.foreach(comment => sb.append(s" comment '$comment'"))
    sb.toString()
  }

  private def decompileCreateView(v: View): String = {
    val sb = new StringBuilder("create view ")
    if (v.ifNotExists) sb.append(" if not exists ")
    sb.append(s" ${v.ref.toSQL}")
    v.description.foreach(comment => sb.append(s" comment '$comment' "))
    sb.append(s" as ${v.query.toSQL}")
    sb.toString()
  }

  private def decompileDelete(d: Delete): String = {
    val sb = new StringBuilder(s"delete from ${d.table}")
    d.where.foreach(where => sb.append(s" where ${where.toSQL}"))
    d.limit.foreach(n => sb.append(s" limit $n"))
    sb.toString()
  }

  private def decompileEntityRef(ref: EntityRef): String = {
    val sb = new StringBuilder()
    ref.databaseName.foreach(name => sb.append(name).append('.'))
    ref.schemaName.foreach(name => sb.append(name).append('.'))
    sb.append(ref.name).toString()
  }

  private def decompileExpression(expression: Expression): String = expression match {
    case AllFields => "*"
    case AND(a, b) => s"${a.toSQL} and ${b.toSQL}"
    case Between(expr, a, b) => s"${expr.toSQL} between ${a.toSQL} and ${b.toSQL}"
    case BasicFieldRef(name) => s"`$name`"
    case ConditionalOp(expr0, expr1, _, sqlOp) => s"${expr0.toSQL} $sqlOp ${expr1.toSQL}"
    case CurrentRow => "current row"
    case Distinct(expressions) => s"distinct ${expressions.map(_.toSQL).mkString(",")}"
    case Exists(query) => s"exists(${query.toSQL})"
    case Following(expr) => s"${expr.toSQL} following"
    case FunctionCall(name, args) => s"$name(${args.map(_.toSQL).mkString(",")})"
    case IN(expr, source) => s"${expr.toSQL} in (${source.toSQL})"
    case Interval(expr, _type) => s"interval ${expr.toSQL} ${_type.toSQL}"
    case IsNotNull(a) => s"${a.toSQL} is not null"
    case IsNull(a) => s"${a.toSQL} is null"
    case JoinFieldRef(name, tableAlias) => tableAlias.map(alias => s"$alias.$name").getOrElse(name)
    case LIKE(a, b) => s"${a.toSQL} like ${b.toSQL}"
    case Literal(value) => decompileLiteral(value)
    case MathOp(expr0, expr1, operator) => s"${expr0.toSQL} $operator ${expr1.toSQL}"
    case NOT(a) => s"not ${a.toSQL}"
    case OR(a, b) => s"${a.toSQL} or ${b.toSQL}"
    case o: Over => decompileOver(o)
    case Preceding(expr) => s"${expr.toSQL} preceding"
    case RLIKE(a, b) => s"${a.toSQL} rlike ${b.toSQL}"
    case ScalarVariableRef(name) => s"$$$name"
    case Unbounded(accessType) => s"${accessType.toSQL} unbounded"
    case WindowBetween(accessType, from, to) => s"${accessType.toSQL} between ${from.toSQL} and ${to.toSQL}"
    case x => fail(x)
  }

  private def decompileInsert(i: Insert): String = {
    val sb = new StringBuilder("insert")
    i.destination match {
      case Insert.Into(target) => sb.append(s" into ${target.toSQL}")
      case Insert.Overwrite(target) => sb.append(s" overwrite ${target.toSQL}")
    }
    if (i.fields.nonEmpty) sb.append(s" (${i.fields.map(_.toSQL).mkString(",")})")
    sb.append(s" ${i.source.toSQL}")
    sb.toString()
  }

  private def decompileJoin(j: Join): String = j match {
    case Join.On(source, condition, _type) => s"${_type.toSQL} ${source.toSQL} on ${condition.toSQL}"
    case Join.Using(source, columns, _type) => s"${_type.toSQL} ${source.toSQL} using ${columns mkString ","}"
    case x => fail(x)
  }

  private def decompileLiteral(value: Any): String = value.asInstanceOf[AnyRef] match {
    case n: Number => n.toString
    case s => s"'$s'"
  }

  private def decompileOver(o: Over): String = {
    val sb = new StringBuilder(s"${o.expression.toSQL} over (")
    if (o.partitionBy.nonEmpty) sb.append(s" partition by ${o.partitionBy.map(_.toSQL) mkString ","}")
    if (o.orderBy.nonEmpty) sb.append(s" order by ${o.orderBy.map(_.toSQL) mkString ","}")
    o.modifier.foreach(modifier => sb.append(s" ${modifier.toSQL}"))
    sb.append(")")
    sb.toString()
  }

  private def decompileSelect(s: Select): String = {
    import s._
    val sb = new StringBuilder(s"select ${fields.map(_.toSQL).mkString(", ")}")
    from.foreach(queryable => sb.append(s" from ${queryable.toSQL}"))
    joins.foreach(join => sb.append(s" ${join.toSQL}"))
    where.foreach(condition => sb.append(s" where ${condition.toSQL}"))
    if (groupBy.nonEmpty) sb.append(s" group by ${groupBy.map(_.toSQL).mkString(", ")}")
    having.foreach(condition => sb.append(s" having ${condition.toSQL}"))
    if (orderBy.nonEmpty) sb.append(s" order by ${orderBy.map(_.toSQL).mkString(", ")}")
    limit.foreach(n => sb.append(s" limit $n"))
    sb.toString()
  }

  private def decompileShow(s: Show): String = {
    val sb = new StringBuilder(s"show ${s.rows.toSQL}")
    s.limit foreach (limit => sb.append(s" limit $limit "))
    sb.toString()
  }

  private def decompileSQL(s: SQL): String = {
    val sb = new StringBuilder("begin\n")
    s.statements.foreach { op =>
      sb.append(s"\t${op.toSQL};\n")
    }
    sb.append("end\n").toString()
  }

  private def decompileUpdate(u: Update): String = {
    val sb = new StringBuilder(s"update ${u.table.toSQL} set ${
      u.changes.map { case (k, v) => s"`$k` = ${v.toSQL}" } mkString ","
    }")
    u.where foreach (where => sb.append(s" where ${where.toSQL}"))
    u.limit foreach (limit => sb.append(s" limit $limit"))
    sb.toString()
  }

  private def fail[A](opCode: AnyRef): A = die(s"Failed to decompile '$opCode' (${opCode.getClass.getName})")

}

/**
 * SQL Decompiler
 */
object SQLDecompiler extends SQLDecompiler {

  object implicits {

    /**
     * SQL Alias Helper
     * @param sql the SQL query or statement
     */
    final implicit class SQLAliasHelper(val sql: String) extends AnyVal {
      def withAlias(alias_? : Option[String]): String = alias_?.map(alias => s"$sql as $alias").getOrElse(sql)
    }

    /**
     * SQL Decompiler Helper
     * @param opCode the [[AnyRef opCode]] to decompile
     */
    final implicit class SQLDecompilerHelper(val opCode: AnyRef) extends AnyVal {
      def toSQL: String = decompile(opCode)
    }

    /**
     * SQL Option Helper
     * @param option the [[Option option]]
     */
    final implicit class SQLOptionHelper(val option: Option[String]) extends AnyVal {
      @inline def orBlank: String = option.getOrElse("")
    }

  }

}

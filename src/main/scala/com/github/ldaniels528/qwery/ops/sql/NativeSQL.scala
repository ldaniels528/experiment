package com.github.ldaniels528.qwery.ops.sql

import java.sql.Connection

import com.github.ldaniels528.qwery.ops.{Executable, Expression, Hints, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.sources.JDBCSupport
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Represents a Native SQL statement
  * @param expression the SQL query or statement expression
  */
case class NativeSQL(expression: Expression, jdbcUrl: String, hints: Option[Hints]) extends Executable with JDBCSupport {
  private var conn_? : Option[Connection] = None

  override def execute(scope: Scope): ResultSet = {
    expression.getAsString(scope).map(scope.expand).map(_.trim) match {
      case Some(sql) => executeSQL(getConnection(scope), sql)
      case None =>
        throw new IllegalArgumentException("No SQL to execute")
    }
  }

  private def getConnection(scope: Scope): Connection = {
    conn_? getOrElse {
      createConnection(scope, jdbcUrl, hints) match {
        case Success(conn) =>
          conn_? = Option(conn)
          conn
        case Failure(e) =>
          throw new IllegalStateException(s"Connection error: ${e.getMessage}", e)
      }
    }
  }

  private def executeSQL(conn: Connection, sql: String) = {
    if (sql.toUpperCase.startsWith("SELECT")) executeQuery(conn, sql)
    else executeStatement(conn, sql)
  }

  private def executeQuery(conn: Connection, sql: String) = {
    val rs = conn.createStatement().executeQuery(sql)
    val columnNames = getColumnNames(rs)
    var rows: List[Row] = Nil
    while (rs.next()) {
      val row = columnNames flatMap { columnName =>
        Option(rs.getObject(columnName)).map(columnName -> _)
      }
      rows = row :: rows
    }
    ResultSet(rows = rows.reverseIterator)
  }

  private def executeStatement(conn: Connection, sql: String) = {
    val count = conn.createStatement().executeUpdate(sql)
    ResultSet.affected(count = count)
  }

}

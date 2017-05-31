package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.github.ldaniels528.qwery.SQLGenerator
import com.github.ldaniels528.qwery.devices.Device
import com.github.ldaniels528.qwery.ops.{Row, Scope}

import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
  * JDBC Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCOutputSource(tableName: String, connect: () => Connection)
  extends OutputSource {
  private val sqlGenerator = new SQLGenerator()
  private var conn_? : Option[Connection] = None
  private val preparedStatements = TrieMap[String, PreparedStatement]()

  override def close(): Unit = {
    preparedStatements.values.foreach(ps => Try(ps.close()))
    conn_?.foreach(_.close())
  }

  override lazy val device = new Device {
    override def close(): Unit = ()
  }

  override def open(scope: Scope): Unit = {
    super.open(scope)
    conn_? = Option(connect())
  }

  override def write(row: Row): Unit = {
    toInsert(row) foreach { ps =>
      row.map(_._2).zipWithIndex foreach { case (value, index) =>
        ps.setObject(index + 1, value)
      }
    }
  }

  private def toInsert(row: Row): Option[PreparedStatement] = {
    val sql = sqlGenerator.insert(tableName, row)
    for {
      conn <- conn_?
    } yield preparedStatements.getOrElseUpdate(sql, conn.prepareStatement(sql))
  }

}

/**
  * JDBC Output Device Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCOutputSource {

  def apply(url: String, tableName: String): JDBCOutputSource = {
    JDBCOutputSource(tableName, () => DriverManager.getConnection(url))
  }

}
package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.github.ldaniels528.qwery.SQLGenerator
import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
  * JDBC Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCOutputSource(url: String, tableName: String, hints: Option[Hints]) extends OutputSource with OutputDevice {
  private val log = LoggerFactory.getLogger(getClass)
  private val preparedStatements = TrieMap[String, PreparedStatement]()
  private val sqlGenerator = new SQLGenerator()
  private var conn_? : Option[Connection] = None
  private var offset = 0L

  override def close(): Unit = {
    preparedStatements.values.foreach(ps => Try(ps.close()))
    conn_?.foreach(_.close())
  }

  override def device: this.type = this

  override def getStatistics: Option[Statistics] = statsGen.update(force = true)

  override def open(scope: Scope): Unit = {
    super.open(scope)
    offset = 0

    // load the driver
    for {
      hints <- hints
      jdbcDriver <- hints.jdbcDriver
    } Class.forName(jdbcDriver).newInstance()

    // open the connection
    conn_? = Option(DriverManager.getConnection(url))
  }

  override def write(record: Record): Any = ()

  override def write(row: Row): Unit = {
    offset += 1
    try {
      toInsert(row) foreach { ps =>
        row.map(_._2).zipWithIndex foreach { case (value, index) =>
          ps.setObject(index + 1, value)
        }
        ps.execute()
      }
    } catch {
      case e: Exception =>
        statsGen.update(failures = 1)
        log.error(s"Record #$offset failed: ${e.getMessage}")
    }
  }

  private def toInsert(row: Row): Option[PreparedStatement] = {
    val sql = sqlGenerator.insert(tableName, row)
    for {
      conn <- conn_?
    } yield preparedStatements.getOrElseUpdate(sql, {
      log.info(s"SQL: $sql")
      conn.prepareStatement(sql)
    })
  }

}

/**
  * JDBC Output Device Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCOutputSource extends OutputDeviceFactory with SourceUrlParser {

  /**
    * Returns a compatible output device for the given URL.
    * @param path the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[OutputDevice output device]]
    */
  override def parseOutputURL(path: String, hints: Option[Hints]): Option[OutputDevice] = {
    if (path.startsWith("jdbc:")) {
      val comps = parseURI(path)
      for {
        tableName <- comps.params.get("table")
        url = path.indexOf('?') match {
          case -1 => path
          case index => path.substring(0, index)
        }
      } yield JDBCOutputSource(url = url, tableName = tableName, hints)
    }
    else None
  }

}
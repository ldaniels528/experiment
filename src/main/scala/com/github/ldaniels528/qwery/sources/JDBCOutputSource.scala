package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, PreparedStatement}

import com.github.ldaniels528.qwery.SQLGenerator
import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

/**
  * JDBC Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCOutputSource(url: String, tableName: String, hints: Option[Hints])
  extends OutputSource with OutputDevice with JDBCSupport {
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

    // open the connection
    createConnection(scope, url, hints) match {
      case Success(conn) =>
        conn_? = Option(conn)
      case Failure(e) =>
        throw new IllegalStateException(s"Connection error: ${e.getMessage}", e)
    }
  }

  override def write(record: Record): Any = ()

  override def write(row: Row): Unit = insert(row) match {
    case Success(count_?) =>
      count_?.foreach { count =>
        offset += 1
        statsGen.update(records = count)
      }
    case Failure(e) =>
      statsGen.update(failures = 1)
      log.error(s"Record #$offset failed: ${e.getMessage}")
  }

  def upsert(row: Row, where: Seq[String]): Try[Option[Int]] = insert(row).recoverWith { case e =>
    if (e.getMessage.toLowerCase().contains("duplicate")) update(row, where)
    else {
      log.warn(s"insert failed: ${e.getMessage}")
      Try(None)
    }
  }

  def insert(row: Row): Try[Option[Int]] = Try {
    val sql = sqlGenerator.insert(tableName, row)
    conn_? map { conn =>
      val ps = preparedStatements.getOrElseUpdate(sql, conn.prepareStatement(sql))
      row.map(_._2).zipWithIndex foreach { case (value, index) =>
        ps.setObject(index + 1, value)
      }
      ps.executeUpdate()
    }
  }

  def update(row: Row, where: Seq[String]): Try[Option[Int]] = Try {
    val sql = sqlGenerator.update(tableName, row, where)
    conn_? map { conn =>
      val ps = preparedStatements.getOrElseUpdate(sql, conn.prepareStatement(sql))
      row.map(_._2).zipWithIndex foreach { case (value, index) =>
        ps.setObject(index + 1, value)
      }
      ps.executeUpdate()
    }
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
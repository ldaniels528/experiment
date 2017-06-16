package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, ResultSet}

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}

import scala.util.{Failure, Success}

/**
  * JDBC Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCInputSource(url: String, table: String, hints: Option[Hints])
  extends InputSource with InputDevice with JDBCSupport {
  private var conn_? : Option[Connection] = None
  private var resultSet: Option[ResultSet] = None
  private var columnNames: Seq[String] = Nil

  override def close(): Unit = conn_?.foreach(_.close())

  override def device: JDBCInputSource = this

  override def getSize: Option[Long] = None

  override def getStatistics: Option[Statistics] = statsGen.update(force = true)

  override def open(scope: Scope): Unit = {
    super.open(scope)

    // open the connection
    getConnection(scope, url, hints) match {
      case Success(conn) =>
        conn_? = Option(conn)
      case Failure(e) =>
        throw new IllegalStateException(s"Connection error: ${e.getMessage}", e)
    }
  }

  override def read(): Option[Record] = None

  override def read(scope: Scope): Option[Row] = {
    resultSet match {
      case Some(rs) =>
        if (rs.next()) Some(columnNames flatMap { name =>
          statsGen.update(records = 1)
          Option(rs.getObject(name)).map(name -> _)
        }) else None
      case None =>
        resultSet = conn_?.map(_.createStatement().executeQuery(generateQuery()))
        columnNames = (resultSet map getColumnNames).getOrElse(throw new IllegalStateException("Column names could not be determined"))

        // read the next record
        read(scope)
    }
  }

  private def generateQuery() = {
    s"SELECT * FROM $table"
  }

}

/**
  * JDBC Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCInputSource extends InputDeviceFactory with SourceUrlParser {

  /**
    * Parses a JDBC URL
    * @param path the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[JDBCInputSource JDBC input source]]
    */
  override def parseInputURL(path: String, hints: Option[Hints]): Option[JDBCInputSource] = {
    if (path.startsWith("jdbc:")) {
      val comps = parseURI(path)
      for {
        table <- comps.params.get("table")
        url = path.indexOf('?') match {
          case -1 => path
          case index => path.substring(0, index)
        }
      } yield JDBCInputSource(url = url, table = table, hints)
    }
    else None
  }

}
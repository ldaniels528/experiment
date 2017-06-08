package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, DriverManager, ResultSet}

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}

/**
  * JDBC Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCInputSource(url: String, query: String, hints: Option[Hints]) extends InputSource {
  private var conn: Option[Connection] = None
  private var resultSet: Option[ResultSet] = None
  private var columnNames: Seq[String] = Nil

  override def close(): Unit = conn.foreach(_.close())

  override lazy val device = JDBCInputDevice(url, query, hints)

  override def open(scope: Scope): Unit = {
    super.open(scope)
    conn = Option(DriverManager.getConnection(url))
  }

  override def read(): Option[Row] = {
    resultSet match {
      case Some(rs) =>
        if (rs.next()) Some(columnNames.map(name => name -> rs.getObject(name))) else None
      case None =>
        resultSet = conn.map(_.createStatement().executeQuery(query))
        columnNames = (resultSet.map(_.getMetaData) map { rsmd =>
          val count = rsmd.getColumnCount
          for (n <- 1 to count) yield rsmd.getCatalogName(n)
        }).getOrElse(throw new IllegalStateException("Column names could not be determined"))

        // read the next record
        read()
    }
  }

}

/**
  * JDBC Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCInputSource extends InputDeviceFactory with InputSourceFactory with SourceUrlParser {

  /**
    * Parses a JDBC URL
    * @param path the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[JDBCInputSource JDBC input source]]
    */
  override def parseInputURL(path: String, hints: Option[Hints]): Option[JDBCInputDevice] = {
    if (path.startsWith("jdbc:")) {
      val comps = parseURI(path)
      for {
        query <- comps.params.get("query")
        url = path.indexOf('?') match {
          case -1 => path
          case index => path.substring(0, index)
        }
      } yield JDBCInputDevice(url = url, query = query, hints)
    }
    else None
  }

  override def findInputSource(device: InputDevice, hints: Option[Hints]): Option[InputSource] = {
    device match {
      case jdbc: JDBCInputSource => Some(jdbc)
      case _ => None
    }
  }

}
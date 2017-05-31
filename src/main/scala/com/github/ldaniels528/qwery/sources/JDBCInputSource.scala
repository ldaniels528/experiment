package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, DriverManager, ResultSet}

import com.github.ldaniels528.qwery.devices.Device
import com.github.ldaniels528.qwery.ops.{Row, Scope}

/**
  * JDBC Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JDBCInputSource(query: String, connect: () => Connection) extends InputSource {
  private var conn: Option[Connection] = None
  private var resultSet: Option[ResultSet] = None
  private var columnNames: Seq[String] = Nil

  override def close(): Unit = conn.foreach(_.close())

  override lazy val device = new Device {
    override def close(): Unit = ()
  }

  override def open(scope: Scope): Unit = {
    super.open(scope)
    conn = Option(connect())
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
  * JDBC Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCInputSource {

  def apply(url: String, tableName: String): JDBCInputSource = {
    JDBCInputSource(query = s"SELECT * FROM $tableName", () => DriverManager.getConnection(url))
  }

}
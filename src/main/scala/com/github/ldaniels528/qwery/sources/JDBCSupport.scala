package com.github.ldaniels528.qwery.sources

import java.sql.{Connection, DriverManager}
import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.ops.{Hints, Scope}

import scala.util.Try

/**
  * JDBC Support
  * @author lawrence.daniels@gmail.com
  */
trait JDBCSupport {

  def getColumnNames(rs: java.sql.ResultSet): Seq[String] = {
    val metaData = rs.getMetaData
    val count = metaData.getColumnCount
    for (n <- 1 to count) yield metaData.getColumnName(n)
  }

  /**
    * Creates a database connection
    * @param scope the given [[Scope scope]]
    * @param url   the given JDBC URL
    * @param hints the given [[Hints hints]]
    * @return the [[Connection connection]]
    */
  def getConnection(scope: Scope, url: String, hints: Option[Hints]) = Try {
    // load the driver
    for {
      hints <- hints
      jdbcDriver <- hints.jdbcDriver
    } Class.forName(jdbcDriver).newInstance()

    // pass any defined properties
    val properties = hints.flatMap(_.properties) getOrElse new JProperties()

    // open the connection
    DriverManager.getConnection(url, properties)
  }

}

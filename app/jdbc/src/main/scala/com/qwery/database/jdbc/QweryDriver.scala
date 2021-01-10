package com.qwery.database
package jdbc

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.qwery.database.server.DatabaseClient

import scala.beans.BeanProperty

/**
 * Qwery JDBC Driver
 */
class QweryDriver extends Driver {
  private val urlPattern = "jdbc:qwery://(\\S+):(\\d+)/(\\S+)".r // (e.g. "jdbc:qwery://localhost:8233/qwery")

  @BeanProperty val majorVersion: Int = 0
  @BeanProperty val minorVersion: Int = 1
  @BeanProperty val parentLogger: Logger = Logger.getLogger(getClass.getName)

  override def acceptsURL(url: String): Boolean = urlPattern.findAllIn(url).nonEmpty

  override def connect(url: String, info: Properties): Connection = {
    url match {
      case urlPattern(host, port, databaseName) =>
        val conn = new JDBCConnection(client = DatabaseClient(host, port.toInt), url = url)
        conn.setCatalog(databaseName)
        conn.setSchema(databaseName)
        conn
      case x => die(s"Invalid JDBC URL: $x")
    }
  }

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    Array(
      new DriverPropertyInfo("database", "qwery"),
      new DriverPropertyInfo("server", "localhost"),
      new DriverPropertyInfo("port", "8233")
    )
  }

  override def jdbcCompliant(): Boolean = false

}

/**
 * Qwery JDBC Driver Companion
 */
object QweryDriver {

  // register the driver
  DriverManager.registerDriver(new QweryDriver())

}
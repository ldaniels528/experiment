package com.qwery.database
package jdbc

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger

import com.qwery.database.server.ClientSideTableService

import scala.beans.BeanProperty

/**
 * Qwery JDBC Driver
 */
class QweryDriver extends Driver {
  private val urlPattern = "jdbc:qwery://(\\S+):(\\d+)/(\\S+)" // (e.g. "jdbc:qwery://localhost:8233/qwery")
  private val urlPatternRegex = urlPattern.r

  @BeanProperty val majorVersion: Int = 0
  @BeanProperty val minorVersion: Int = 1
  @BeanProperty val parentLogger: Logger = Logger.getLogger(getClass.getName)

  override def acceptsURL(url: String): Boolean = urlPattern.matches(url)

  override def connect(url: String, info: Properties): Connection = {
    url match {
      case urlPatternRegex(host, port, database) =>
        new JDBCConnection(service = ClientSideTableService(host, port.toInt), database = database, url = url)
      case x => die(s"Invalid JDBC URL: $x")
    }
  }

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    Array(
      new DriverPropertyInfo("majorVersion", majorVersion.toString),
      new DriverPropertyInfo("minorVersion", minorVersion.toString)
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
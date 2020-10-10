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
object QweryDriver extends Driver {
  private val urlPattern = "jdbc:qwery://(\\S+):(\\d+)/(\\S+)" // (e.g. "jdbc:qwery://localhost:12122/qwery")

  @BeanProperty val majorVersion: Int = 0
  @BeanProperty val minorVersion: Int = 1
  @BeanProperty val parentLogger: Logger = Logger.getLogger(getClass.getName)

  // register the driver
  DriverManager.registerDriver(this)

  override def acceptsURL(url: String): Boolean = urlPattern.matches(url)

  override def connect(url: String, info: Properties): Connection = {
    val regex = urlPattern.r
    url match {
      case regex(host, port, database) =>
        val service = ClientSideTableService(host, port.toInt)
        new JDBCConnection(service, database, url)
      case x => throw new IllegalArgumentException(s"Invalid JDBC URL: $x")
    }
  }

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    Array(
      new DriverPropertyInfo("majorVersion", majorVersion.toString),
      new DriverPropertyInfo("minorVersion", minorVersion.toString)
    )
  }

  override def jdbcCompliant(): Boolean = true

}
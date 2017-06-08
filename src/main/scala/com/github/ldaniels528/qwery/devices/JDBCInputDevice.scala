package com.github.ldaniels528.qwery.devices

import com.github.ldaniels528.qwery.ops.Hints

/**
  * JDBC Input Device
  * @author lawrence.daniels@gmail.com
  */
case class JDBCInputDevice(url: String, query: String, hints: Option[Hints]) extends InputDevice {

  override def read(): Option[Record] = None

  override def getSize: Option[Long] = None

  override def close(): Unit = {}
}

/**
  * JDBC Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object JDBCInputDevice extends InputDeviceFactory with SourceUrlParser {

  /**
    * Returns a compatible input device for the given URL.
    * @param url the given URL (e.g. "jdbc:mysql://localhost/test?query=...")
    * @return an option of the [[JDBCInputDevice input device]]
    */
  override def parseInputURL(url: String, hints: Option[Hints]): Option[JDBCInputDevice] = {
    val comps = parseURI(url)
    for {
      query <- comps.params.get("query")
      jdbcUrl = url.substring(0, url.indexOf('?'))
    } yield JDBCInputDevice(url = jdbcUrl, query = query, hints = hints)
  }

}
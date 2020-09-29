package com.qwery.database.server

import java.net.{HttpURLConnection, URL}

import com.qwery.util.ResourceHelper._

import scala.concurrent.duration.{Duration, DurationInt}
import scala.io.Source

/**
 * Qwery HTTP Client
 */
class QweryHttpClient(connectionTimeout: Duration = 1.second, readTimeout: Duration = 1.second) {
  import net.liftweb.json.{DefaultFormats, JValue, parse}

  def delete[T](url: String)(implicit m: Manifest[T]): T = fromJSON[T](deleteJSON(url))

  def deleteJSON(url: String): JValue = httpXXX(method = "DELETE", url)

  def get[T](url: String)(implicit m: Manifest[T]): T = fromJSON[T](getJSON(url))

  def getJSON(url: String): JValue = httpXXX(method = "GET", url)

  def patch[T](url: String)(implicit m: Manifest[T]): T = fromJSON[T](putJSON(url))

  def patchJSON(url: String): JValue = httpXXX(method = "PATCH", url)

  def post[T](url: String)(implicit m: Manifest[T]): T = fromJSON[T](postJSON(url))

  def post[T](url: String, content: Array[Byte])(implicit m: Manifest[T]): T = fromJSON[T](postJSON(url, content))

  def postJSON(url: String): JValue = httpXXX(method = "POST", url)

  def postJSON(url: String, content: Array[Byte]): JValue = httpXXX(method = "POST", url, content)

  def put[T](url: String)(implicit m: Manifest[T]): T = fromJSON[T](putJSON(url))

  def putJSON(url: String): JValue = httpXXX(method = "PUT", url)

  private def httpXXX(method: String, url: String): JValue = {
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        val jsonString = Source.fromInputStream(conn.getInputStream).use(_.mkString)
        toJSON(jsonString)
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn")
    }
  }

  private def httpXXX(method: String, url: String, content: Array[Byte]): JValue = {
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setReadTimeout(readTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setRequestProperty("Accept", "application/json")
        conn.setRequestProperty("User-Agent", "Mozilla/5.0")
        conn.setDoOutput(true)
        conn.setDoInput(true)
        //conn.setFixedLengthStreamingMode(content.length)
        //conn.connect()
        conn.getOutputStream.use(_.write(content))
        conn.getResponseCode match {
          case HttpURLConnection.HTTP_OK => toJSON(Source.fromInputStream(conn.getInputStream).use(_.mkString))
          case code => throw new IllegalStateException(s"Server Error HTTP/$code: ${conn.getResponseMessage}")
        }
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn")
    }
  }

  private def fromJSON[T](jValue: JValue)(implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats = DefaultFormats
    jValue.extract[T]
  }

  private  def toJSON(jsonString: String): JValue = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(jsonString)
  }

}

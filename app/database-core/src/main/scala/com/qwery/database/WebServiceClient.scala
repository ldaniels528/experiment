package com.qwery.database

import com.qwery.util.ResourceHelper._
import net.liftweb.json.{DefaultFormats, JObject, JValue, parse}
import org.apache.commons.io.IOUtils

import java.io._
import java.net.{HttpURLConnection, URL}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.io.Source

/**
 * Qwery Web Service Client
 */
class WebServiceClient(connectionTimeout: Duration = 5.second, readTimeout: Duration = 15.second) {

  /**
   * The HTTP DELETE request method deletes the specified resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>May</td></tr>
   * <tr><td>Successful response has body</td> <td>May</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   * @return the response as a [[JValue JSON value]]
   */
  def delete(url: String): JValue = httpXXX(method = "DELETE", url)

  /**
   * The HTTP DELETE request method deletes the specified resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>May</td></tr>
   * <tr><td>Successful response has body</td> <td>May</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   * @param body the request body
   * @return the response as a [[JValue JSON value]]
   */
  def delete(url: String, body: Array[Byte]): JValue = httpXXX(method = "DELETE", url, body, doInput = false)

  /**
   * The HTTP DELETE request method deletes the specified resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>May</td></tr>
   * <tr><td>Successful response has body</td> <td>May</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url  the web service URL
   * @param file the request body as a [[File file]]
   * @return the response as a [[JValue JSON value]]
   */
  def delete(url: String, file: File): JValue = httpXXX(method = "DELETE", url, file, doInput = false)

  /**
   * The HTTP GET method requests a representation of the specified resource. Requests using GET should only retrieve data.
   * <table border="1">
   * <tr><td>Request has body</td> <td>No</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>Yes</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>Yes</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>Yes</td></tr>
   * </table>
   * @param url the web service URL
   * @return the response as a [[JValue JSON value]]
   */
  def get(url: String): JValue = httpXXX(method = "GET", url)

  /**
   * Downloads the resource as a byte array
   * @param url the URL that describes the resource
   * @return a byte array representing the content
   */
  def getAsBytes(url: String): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    httpDownload(url, out)
    out.toByteArray
  }

  /**
   * Downloads the resource as a byte array
   * @param url the URL that describes the resource
   * @return a [[File file]] containing the content
   */
  def getAsFile(url: String): File = {
    val file = File.createTempFile("qwery", "download")
    new FileOutputStream(file) use { out => httpDownload(url, out) }
    file
  }

  /**
   * The HTTP PATCH request method applies partial modifications to a resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   * @return the response as a [[JValue JSON value]]
   */
  def patch(url: String): JValue = httpXXX(method = "PATCH", url)

  /**
   * The HTTP PATCH request method applies partial modifications to a resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   * @param body the request body
   * @return the response as a [[JValue JSON value]]
   */
  def patch(url: String, body: Array[Byte]): JValue = httpXXX(method = "PATCH", url, body, doInput = true)

  /**
   * The HTTP PATCH request method applies partial modifications to a resource.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   * @param file the request body as a [[File file]]
   * @return the response as a [[JValue JSON value]]
   */
  def patch(url: String, file: File): JValue = httpXXX(method = "PATCH", url, file, doInput = true)

  /**
   * The HTTP POST method sends data to the server.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>Only if freshness information is included</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>Yes</td></tr>
   * </table>
   * @param url the web service URL
   * @return the response as a [[JValue JSON value]]
   */
  def post(url: String): JValue = httpXXX(method = "POST", url)

  /**
   * The HTTP POST method sends data to the server. The type of the body of the request is indicated
   * by the Content-Type header.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>Only if freshness information is included</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>Yes</td></tr>
   * </table>
   * @param url  the web service URL
   * @param body the request body
   * @return the response as a [[JValue JSON value]]
   */
  def post(url: String, body: Array[Byte]): JValue = httpXXX(method = "POST", url, body, doInput = true)

  /**
   * The HTTP POST method sends data to the server. The type of the body of the request is indicated
   * by the Content-Type header.
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>Yes</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>No</td></tr>
   * <tr><td>Cacheable</td> <td>Only if freshness information is included</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>Yes</td></tr>
   * </table>
   * @param url  the web service URL
   * @param file the request body as a [[File file]]
   * @return the response as a [[JValue JSON value]]
   */
  def post(url: String, file: File): JValue = httpXXX(method = "POST", url, file, doInput = true)

  /**
   * Performs an HTTP PUT request
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>No</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url the web service URL
   */
  def put(url: String): Unit = httpXXX(method = "PUT", url)

  /**
   * Performs an HTTP PUT request
   * <table border="1">
   *  <tr><td>Request has body</td> <td>Yes</td></tr>
   *  <tr><td>Successful response has body</td> <td>No</td></tr>
   *  <tr><td>Safe</td> <td>No</td></tr>
   *  <tr><td>Idempotent</td> <td>Yes</td></tr>
   *  <tr><td>Cacheable</td> <td>No</td></tr>
   *  <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url     the web service URL
   * @param body the request body
   */
  def put(url: String, body: Array[Byte]): Unit = httpXXX(method = "PUT", url, body, doInput = false)

  /**
   * Performs an HTTP PUT request
   * <table border="1">
   * <tr><td>Request has body</td> <td>Yes</td></tr>
   * <tr><td>Successful response has body</td> <td>No</td></tr>
   * <tr><td>Safe</td> <td>No</td></tr>
   * <tr><td>Idempotent</td> <td>Yes</td></tr>
   * <tr><td>Cacheable</td> <td>No</td></tr>
   * <tr><td>Allowed in HTML forms</td> <td>No</td></tr>
   * </table>
   * @param url  the web service URL
   * @param file the request body as a [[File file]]
   */
  def put(url: String, file: File): Unit = httpXXX(method = "PUT", url, file, doInput = false)

  private def httpDownload(url: String, out: OutputStream): Int = {
    val method = "GET"
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        conn.setRequestProperty("Connection", "keep-alive")
        conn.getInputStream.use(in => IOUtils.copy(in, out))
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn [$method|$url]")
    }
  }

  private def httpXXX(method: String, url: String): JValue = {
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        conn.setRequestProperty("Connection", "keep-alive")
        toJSON(jsonString = Source.fromInputStream(conn.getInputStream).use(_.mkString))
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn [$method|$url]")
    }
  }

  private def httpXXX(method: String, url: String, body: Array[Byte], doInput: Boolean): JValue = {
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setReadTimeout(readTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        conn.setRequestProperty("Accept", "application/json")
        conn.setRequestProperty("Connection", "keep-alive")
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
        conn.setRequestProperty("User-Agent", "Mozilla/5.0")
        conn.setDoOutput(true)
        if (doInput) conn.setDoInput(doInput)
        conn.getOutputStream.use(_.write(body))
        conn.getResponseCode match {
          case HttpURLConnection.HTTP_OK =>
            if (doInput) toJSON(Source.fromInputStream(conn.getInputStream).use(_.mkString)) else JObject()
          case code =>
            throw new IllegalStateException(s"Server Error HTTP/$code: ${conn.getResponseMessage} [$method|$url]")
        }
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn [$method|$url]")
    }
  }

  private def httpXXX(method: String, url: String, file: File, doInput: Boolean): JValue = {
    new URL(url).openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(connectionTimeout.toMillis.toInt)
        conn.setReadTimeout(readTimeout.toMillis.toInt)
        conn.setRequestMethod(method)
        conn.setRequestProperty("Accept", "application/json")
        conn.setRequestProperty("Connection", "keep-alive")
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
        conn.setRequestProperty("User-Agent", "Mozilla/5.0")
        conn.setDoOutput(true)
        if (doInput) conn.setDoInput(doInput)
        new FileInputStream(file).use(in => conn.getOutputStream.use(out => IOUtils.copy(in, out)))
        conn.getResponseCode match {
          case HttpURLConnection.HTTP_OK =>
            if (doInput) toJSON(Source.fromInputStream(conn.getInputStream).use(_.mkString)) else JObject()
          case code =>
            throw new IllegalStateException(s"Server Error HTTP/$code: ${conn.getResponseMessage} [$method|$url]")
        }
      case conn =>
        throw new IllegalArgumentException(s"Invalid connection type $conn [$method|$url]")
    }
  }

  private def toJSON(jsonString: String): JValue = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(jsonString)
  }

}

/**
 * Qwery Web Service Companion
 */
object WebServiceClient {

  final implicit class QweryResponseConversion(val response: JValue) extends AnyVal {

    @inline
    def as[T](implicit m: Manifest[T]): T = {
      implicit val formats: DefaultFormats = DefaultFormats
      response.extract[T]
    }
  }

}
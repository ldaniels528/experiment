package com.github.ldaniels528.qwery.util

import java.io.File

import com.github.ldaniels528.qwery.ops.{Column, Row}
import play.api.libs.json._

import scala.io.Source
import scala.language.postfixOps

/**
  * JSON Support
  * @author lawrence.daniels@gmail.com
  */
trait JSONSupport {

  /**
    * Parses the given JSON file into the specified type
    * @param file the given [[File file]]
    * @tparam T the specified type
    * @return an instance of the specified type
    */
  def parseJsonAs[T](file: File)(implicit m: Manifest[T]): T = parseJsonAs[T](Source.fromFile(file).mkString)

  /**
    * Parses the given JSON string into the specified type
    * @param jsonString the given JSON string
    * @tparam T the specified type
    * @return an instance of the specified type
    */
  def parseJsonAs[T](jsonString: String)(implicit m: Manifest[T]): T = {
    import net.liftweb.json

    implicit val defaults = json.DefaultFormats
    json.parse(jsonString).extract[T]
  }

  /**
    * Parses the given JSON string into a row
    * @param jsonString the given JSON string
    * @return the resultant [[Row row]]
    */
  def parseRow(jsonString: String): Row = parseColumns(parseJson(jsonString), parent = Nil)

  /**
    * Parses the given JSON string into a collection of rows
    * @param jsonString the given JSON string
    * @param jsonPath   the given JSON filtering path
    * @return the resultant [[Row rows]]
    */
  def parseRows(jsonString: String, jsonPath: Seq[String]): List[Row] = {
    val jsRoot = jsonPath.foldLeft[JsValue](parseJson(jsonString)) { (jv, path) => (jv \ path).get }
    jsRoot match {
      case JsArray(rows) => rows.toList.map(row => parseColumns(row, parent = Nil): Row)
      case value => (parseColumns(value, parent = Nil): Row) :: Nil
    }
  }

  /**
    * Transforms the given row into a JSON string
    * @param row the given [[Row row]]
    * @return a JSON string
    */
  def toJson(row: Row): String = {
    import net.liftweb.json.Extraction.decompose
    import net.liftweb.json._

    implicit val formats = DefaultFormats
    compactRender(decompose(Map(row.columns: _*)))
  }

  /**
    * Parses the given JSON string into a JSON value
    * @param jsonString the given JSON string
    * @return the [[JsValue JSON value]]
    */
  private def parseJson(jsonString: String): JsValue = Json.parse(jsonString)

  /**
    * Parses the given JSON string into a collection of columns
    * @param jsonValue the given [[JsValue JSON value]]
    * @return a collection of [[Column columns]]
    */
  private def parseColumns(jsonValue: JsValue, parent: List[String]): List[Column] = {

    def unwrap(jsValue: JsValue): Any = jsValue match {
      case JsArray(values) => values map unwrap
      case JsBoolean(value) => value
      case JsNull => null
      case JsNumber(value) => value
      case JsObject(mapping) => mapping map { case (key, value) => key -> unwrap(value) }
      case JsString(value) => value
      case js =>
        throw new IllegalArgumentException(s"Unhandled object '$js' (${js.getClass.getName})")
    }

    jsonValue match {
      case JsArray(rows) => rows.zipWithIndex.toList.flatMap { case (row, index) => parseColumns(row, index.toString :: parent) }
      case JsObject(mapping) => mapping.toList flatMap {
        case (name, JsArray(values)) => values.toList.flatMap(v => parseColumns(v, parent = name :: parent))
        case (name, JsObject(values)) => values.toList.flatMap { case (k, v) => parseColumns(v, parent = k :: name :: parent) }
        case (name, value) => parseColumns(value, parent = name :: parent)
      }
      case jsValue => parent.reverse.mkString(".") -> unwrap(jsValue) :: Nil
    }
  }

}

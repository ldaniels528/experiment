package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.ops.{Column, Row}
import play.api.libs.json._

import scala.language.postfixOps

/**
  * JSON Support
  * @author lawrence.daniels@gmail.com
  */
trait JSONSupport {

  def parseRows(jsonValue: JsValue): List[Row] = jsonValue match {
    case JsArray(rows) => rows.toList.map(row => parseColumns(row, parent = Nil): Row)
    case jsValue => (parseColumns(jsValue, parent = Nil): Row) :: Nil
  }

  def parseColumns(jsonValue: JsValue, parent: List[String]): List[Column] = jsonValue match {
    case JsArray(rows) => rows.zipWithIndex.toList.flatMap { case (row, index) => parseColumns(row, index.toString :: parent) }
    case JsObject(mapping) => mapping.toList flatMap {
      case (name, JsArray(values)) => values.toList.flatMap(v => parseColumns(v, parent = name :: parent))
      case (name, JsObject(values)) => values.toList.flatMap { case (k, v) => parseColumns(v, parent = k :: name :: parent) }
      case (name, value) => parseColumns(value, parent = name :: parent)
    }
    case jsValue => parent.reverse.mkString(".") -> unwrap(jsValue) :: Nil
  }

  def unwrap(jsValue: JsValue): Any = jsValue match {
    case JsBoolean(value) => value
    case JsNumber(value) => value
    case JsNull => null
    case JsString(value) => value
    case js =>
      throw new IllegalArgumentException(s"Unhandled object '$js' (${js.getClass.getName})")
  }

}

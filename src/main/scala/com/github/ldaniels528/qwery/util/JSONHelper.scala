package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.ops.{NamedExpression, Row}
import net.liftweb.json.JsonAST._
import net.liftweb.json.{JObject, JValue}

import scala.language.postfixOps

/**
  * JSON Helper
  * @author lawrence.daniels@gmail.com
  */
object JSONHelper {

  def parseResults(jValue: JValue): List[Row] = {
    jValue match {
      case ja: JArray => parseArray(ja)
      case jo: JObject => parseObject(jo)
      case jv => List(Seq(NamedExpression.randomName() -> unwrap(jv)))
    }
  }

  private def parseArray(jArray: JArray): List[Row] = {
    jArray.arr map {
      case jo: JObject => flattenMap(jo.values)
      case jv => Seq(NamedExpression.randomName() -> unwrap(jv))
    }
  }

  private def parseObject(jObject: JObject, path: List[String] = Nil): List[Row] = {
    jObject.values match {
      case m if m.size == 1 && m.values.headOption.exists(_.isInstanceOf[List[_]]) =>
        m.headOption.map { case (key, value) => key -> value.asInstanceOf[List[_]] } map {
          case (key, list: List[_]) => list map {
            case jo: JObject => flattenMap(jo.values, key :: path)
            case jv: JValue => Seq(asKey(key :: path) -> unwrap(jv))
            case map: Map[String, _] => flattenMap(map, key :: path)
            case jv => Seq(asKey(key :: path) -> jv)
          }
        } orNull
      case m =>
        m map {
          case (key, ja: JArray) => flattenList(ja.arr, key :: path)
          case (key, jo: JObject) => flattenMap(jo.values, key :: path)
          case (key, jv: JValue) => Seq(asKey(key :: path) -> unwrap(jv))
          case (key, list: List[_]) => flattenList(list, key :: path)
          case (key, map: Map[String, _]) => flattenMap(map, key :: path)
          case (key, value) => Seq(asKey(key :: path) -> value)
        } toList
    }
  }

  private def asIndex(index: Int): String = f"idx${index + 1}%02d"

  private def asKey(path: List[String]): String = path.reverse.mkString(".")

  private def flattenList(list: List[Any], path: List[String] = Nil): Row = {
    list.zipWithIndex flatMap {
      case (jo: JObject, index) => flattenMap(jo.values, asIndex(index) :: path)
      case (jv: JValue, index) => Seq(asKey(asIndex(index) :: path) -> unwrap(jv))
      case (map: Map[String, _], index) => flattenMap(map, asIndex(index) :: path)
      case (jv, index) => Seq(asKey(asIndex(index) :: path) -> jv)
    }
  }

  private def flattenMap(map: Map[String, Any], path: List[String] = Nil): Row = {
    map flatMap {
      case (key, ja: JArray) => flattenList(ja.arr, key :: path)
      case (key, jo: JObject) => flattenMap(jo.values, key :: path)
      case (key, jv: JValue) => Seq(asKey(key :: path) -> unwrap(jv))
      case (key, list: List[_]) => flattenList(list, key :: path)
      case (key, map: Map[String, _]) => flattenMap(map, key :: path)
      case (key, value) => Seq(asKey(key :: path) -> value)
    } toSeq
  }

  /**
    * Converts the given [[JValue]] into Scala data type
    * @param jValue the given [[JValue]]
    * @return a Scala data type
    */
  private def unwrap(jValue: JValue): AnyRef = jValue match {
    case JNothing | JNull => null
    case a: JArray => a.arr.map(unwrap)
    case b: JBool => b.value: java.lang.Boolean
    case n: JDouble => n.values: java.lang.Double
    case n: JInt => n.values.bigInteger
    case o: JObject => unwrap(o)
    case s: JString => s.values
  }

}

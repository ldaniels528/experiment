package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.ops.{NamedExpression, Row}
import net.liftweb.json.JsonAST._
import net.liftweb.json.{JObject, JValue}
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/**
  * JSON Helper
  * @author lawrence.daniels@gmail.com
  */
object JSONHelper {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Converts the given [[JObject]] into Row object
    * @param jObject the given [[JObject]]
    * @return a [[Row row]] object
    */
  def unwrap(jObject: JObject): Row = jObject.values map {
    case (key, value: JValue) => key -> unwrap(value)
    case (key, value) => key -> value
  } toSeq

  /**
    * Converts the given [[JArray]] into a collection of Row objects
    * @param jArray the given [[JObject]]
    * @return a collection of [[Row row]] objects
    */
  def unwrap(jArray: JArray): List[Row] = {
    jArray.arr.map {
      case jo: JObject => unwrap(jo)
      case jv: JValue => Seq(NamedExpression.randomName() -> unwrap(jv))
    }
  }

  /**
    * Converts the given [[JValue]] into Scala data type
    * @param jValue the given [[JValue]]
    * @return a Scala data type
    */
  def unwrap(jValue: JValue): AnyRef = jValue match {
    case JNothing | JNull => null
    case a: JArray => a.arr.map(unwrap)
    case b: JBool => b.value: java.lang.Boolean
    case n: JDouble => n.values: java.lang.Double
    case n: JInt => n.values.bigInteger
    case o: JObject => unwrap(o)
    case s: JString => s.values
  }

}

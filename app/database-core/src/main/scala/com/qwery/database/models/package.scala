package com.qwery.database

import com.qwery.models.expressions.implicits._
import com.qwery.models.expressions.{Expression, Null}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

package object models {

  /**
    * JsValue Conversion (Spray)
    * @param jsValue the [[JsValue JSON value]]
    */
  final implicit class JsValueConversion(val jsValue: JsValue) extends AnyVal {
    @inline
    def unwrapJSON: Any = jsValue match {
      case js: JsArray => js.elements.map(_.unwrapJSON)
      case JsBoolean(value) => value
      case JsNull => null
      case JsNumber(value) => value
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON }
      case JsString(value) => value
      case x => die(s"Unsupported type $x (${x.getClass.getName})")
    }

    @inline
    def toExpression: Expression = jsValue match {
      case js: JsArray => js.elements.map(_.unwrapJSON)
      case JsBoolean(value) => value
      case JsNull => Null
      case JsNumber(value) => value.toDouble
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON }
      case JsString(value) => value
      case x => die(s"Unsupported type $x (${x.getClass.getName})")
    }
  }

  final implicit class AnyToSprayJsConversion(val value: Any) extends AnyVal {
    @inline
    def toSprayJs: JsValue = value.asInstanceOf[AnyRef] match {
      case null => JsNull
      case b: java.lang.Boolean => JsBoolean(b)
      case m: Map[String, Any] => new JsObject(m.map { case (k, v) => (k, v.toSprayJs) })
      case n: Number => JsNumber(n.doubleValue())
      case s: Seq[Any] => JsArray(s.map(_.toSprayJs): _*)
      case s: String => JsString(s)
      case x => die(s"Unsupported type $x (${x.getClass.getName})")
    }
  }

}

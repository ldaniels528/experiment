package com.qwery.database

import java.io.File

import net.liftweb.json.JsonAST.JField
import net.liftweb.json.{JArray, JBool, JDouble, JInt, JNothing, JNull, JObject, JString, JValue}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

/**
 * Qwery server package object
 */
package object server {

  // miscellaneous constants
  val DEFAULT_DATABASE = "qwery"

  /**
   * Represents a row; a collection of tuples
   */
  type TupleSet = Map[String, Any]

  /**
   * Executes the block capturing its execution the time in milliseconds
   * @param block the block to execute
   * @return a tuplee containing the result of the block and its execution the time in milliseconds
   */
  def time[A](block: => A): (A, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val finishTime = System.nanoTime()
    val elapsedTime = (finishTime - startTime) / 1e+6
    (result, elapsedTime)
  }

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
      case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
    }
  }

  /**
   * JValue Conversion (Lift JSON)
   * @param jValue the [[JValue JSON value]]
   */
  final implicit class JValueConversion(val jValue: JValue) extends AnyVal {
    @inline
    def toSprayJs: JsValue = jValue match {
      case JArray(values) => new JsArray(values.toVector.map(_.toSprayJs))
      case JBool(value) => JsBoolean(value)
      case JDouble(value) => JsNumber(value)
      case JInt(value) => JsNumber(value)
      case JNull | JNothing => JsNull
      case JObject(values) => new JsObject(Map(values.map { case JField(k, v) => (k, v.toSprayJs) }: _*))
      case JString(value) => JsString(value)
      case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
    }

    @inline
    def unwrapJSON: Any = jValue match {
      case JArray(array) => array.map(_.unwrapJSON)
      case JBool(value) => value
      case JDouble(value) => value
      case JInt(value) => value
      case JNull | JNothing => null
      case js: JObject => js.values
      case JString(value) => value
      case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
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
      case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
    }
  }

  /**
   * Qwery File
   * @param theFile the [[File file]]
   */
  final implicit class QweryFile(val theFile: File) extends AnyVal {

    /**
     * Recursively retrieves all files
     * @return the list of [[File files]]
     */
    def listFilesRecursively: List[File] = theFile match {
      case directory if directory.isDirectory => directory.listFiles().toList.flatMap(_.listFilesRecursively)
      case file => file :: Nil
    }

  }

}

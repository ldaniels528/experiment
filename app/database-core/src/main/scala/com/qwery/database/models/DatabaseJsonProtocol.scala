package com.qwery.database
package models

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models.{StorageFormats, TypeAsEnum}
import spray.json._

import java.util.{Date, UUID}

/**
 * Database JSON Protocol
 */
object DatabaseJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  import spray.json._

  def unwrap(jsValue: JsValue): Any = jsValue match {
    case js: JsArray => js.elements map unwrap
    case JsNull => null
    case JsBoolean(value) => value
    case JsNumber(value) => value
    case js: JsObject => js.fields map { case (name, jsValue) => name -> unwrap(jsValue) }
    case JsString(value) => value
    case x => die(s"Unsupported type $x (${x.getClass.getName})")
  }

  ////////////////////////////////////////////////////////////////////////
  //      Utility Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit object OptionAnyJsonFormat extends JsonFormat[Option[Any]] {
    override def read(json: JsValue): Option[Any] = Option(unwrap(json))

    override def write(value: Option[Any]): JsValue = {
      value match {
        case Some(v: Boolean) => v.toJson
        case Some(v: Byte) => v.toJson
        case Some(v: Date) => v.getTime.toJson
        case Some(v: Double) => v.toJson
        case Some(v: Float) => v.toJson
        case Some(v: Int) => v.toJson
        case Some(v: Long) => v.toJson
        case Some(v: Short) => v.toJson
        case Some(v: String) => v.toJson
        case Some(v: UUID) => v.toString.toJson
        case Some(v: Any) => JsString(v.toString)
        case None => JsNull
      }
    }
  }

  implicit object SeqSeqOptionAnyJsonFormat extends JsonFormat[Seq[Seq[Option[Any]]]] {
    override def read(json: JsValue): Seq[Seq[Option[Any]]] = json match {
      case JsArray(rowsJs) => rowsJs collect {
        case JsArray(colsJs) => colsJs.map(v => Option(unwrap(v)))
      }
    }

    override def write(rows: Seq[Seq[Option[Any]]]): JsValue = {
      JsArray((for {row <- rows; array = JsArray(row.map(_.toJson): _*)} yield array): _*)
    }
  }

  ////////////////////////////////////////////////////////////////////////
  //      Enumeration Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit object StorageFormatJsonFormat extends JsonFormat[StorageFormat] {
    override def read(json: JsValue): StorageFormat = StorageFormats.withName(json.convertTo[String])

    override def write(storageFormat: StorageFormat): JsValue = JsString(storageFormat.toString)
  }

  ////////////////////////////////////////////////////////////////////////
  //      Model Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit val typeAsEnumJsonFormat: RootJsonFormat[TypeAsEnum] = jsonFormat2(TypeAsEnum.apply)

}

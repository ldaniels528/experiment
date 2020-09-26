package com.qwery.database.server

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.database.server.TableServices.{TableColumn, TableStatistics}
import spray.json.DefaultJsonProtocol

/**
 * Qwery Custom JSON Protocol
 */
object QweryCustomJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  import spray.json._

  implicit val tableColumnJsonFormat: RootJsonFormat[TableColumn] = jsonFormat8(TableColumn.apply)

  implicit val tableStatisticsJsonFormat: RootJsonFormat[TableStatistics] = jsonFormat5(TableStatistics.apply)

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    override def read(value: JsValue): Map[String, Any] = ???

    override def write(m: Map[String, Any]): JsValue = {
      JsObject(m.mapValues {
        case b: Boolean => if (b) JsTrue else JsFalse
        case d: java.util.Date => JsNumber(d.getTime)
        case n: Double => JsNumber(n)
        case n: Float => JsNumber(n)
        case n: Int => JsNumber(n)
        case n: Long => JsNumber(n)
        case n: Number => JsNumber(n.doubleValue())
        case n: Short => JsNumber(n)
        case s: String => JsString(s)
        case v: Any => JsString(v.toString)
      })
    }
  }

  implicit object SeqMapJsonFormat extends JsonFormat[Seq[Map[String, Any]]] {
    override def read(value: JsValue): Seq[Map[String, Any]] = ???

    override def write(m: Seq[Map[String, Any]]): JsValue = JsArray(m.map(_.toJson):_*)
  }

}

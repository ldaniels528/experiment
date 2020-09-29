package com.qwery.database.server

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.database.BlockDevice.RowStatistics
import com.qwery.database.server.TableFile.{TableColumn, TableStatistics}
import com.qwery.database.server.TableService.UpdateResult
import spray.json.DefaultJsonProtocol

/**
 * Qwery Custom JSON Protocol
 */
object QweryCustomJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  import spray.json._

  def unwrap(jsValue: JsValue): Any = jsValue match {
    case js: JsArray => js.elements map unwrap
    case JsNull => null
    case JsBoolean(value) => value
    case JsNumber(value) => value
    case js: JsObject => js.fields map { case (name, jsValue) => name -> unwrap(jsValue) }
    case JsString(value) => value
    case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
  }

  implicit val rowStatisticsJsonFormat: RootJsonFormat[RowStatistics] = jsonFormat4(RowStatistics.apply)

  implicit val tableColumnJsonFormat: RootJsonFormat[TableColumn] = jsonFormat9(TableColumn.apply)

  implicit val tableStatisticsJsonFormat: RootJsonFormat[TableStatistics] = jsonFormat6(TableStatistics.apply)

  implicit val updateCountJsonFormat: RootJsonFormat[UpdateResult] = jsonFormat3(UpdateResult.apply)

  implicit object TupleSetJsonFormat extends JsonFormat[TupleSet] {
    override def read(jsValue: JsValue): TupleSet = jsValue match {
      case js: JsObject => js.fields map { case (name, jsValue) => name -> unwrap(jsValue) }
      case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
    }

    override def write(m: TupleSet): JsValue = {
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

  implicit object SeqTupleSetJsonFormat extends JsonFormat[Seq[TupleSet]] {
    override def read(value: JsValue): Seq[TupleSet] = ???

    override def write(m: Seq[TupleSet]): JsValue = JsArray(m.map(_.toJson):_*)
  }

}

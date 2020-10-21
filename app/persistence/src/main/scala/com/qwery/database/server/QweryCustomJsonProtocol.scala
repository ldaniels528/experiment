package com.qwery.database.server

import java.util.{Date, UUID}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.database.BlockDevice.RowStatistics
import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.server.TableService._
import com.qwery.database.{ColumnTypes, Field, FieldMetadata, Row, RowMetadata}
import com.qwery.models.TypeAsEnum
import spray.json._

/**
 * Qwery Custom JSON Protocol
 */
object QweryCustomJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  import spray.json._

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
  //      Model Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit val databaseConfigJsonFormat: RootJsonFormat[DatabaseConfig] = jsonFormat1(DatabaseConfig.apply)

  implicit val databaseMetricsJsonFormat: RootJsonFormat[DatabaseMetrics] = jsonFormat3(DatabaseMetrics.apply)

  implicit val loadMetricsJsonFormat: RootJsonFormat[LoadMetrics] = jsonFormat3(LoadMetrics.apply)

  implicit val rowStatisticsJsonFormat: RootJsonFormat[RowStatistics] = jsonFormat6(RowStatistics.apply)

  implicit val tableCreationJsonFormat: RootJsonFormat[TableCreation] = jsonFormat2(TableCreation.apply)

  implicit val tableColumnJsonFormat: RootJsonFormat[TableColumn] = jsonFormat10(TableColumn.apply)

  implicit val tableConfigJsonFormat: RootJsonFormat[TableConfig] = jsonFormat2(TableConfig.apply)

  implicit val tableIndexJsonFormat: RootJsonFormat[TableIndexRef] = jsonFormat2(TableIndexRef.apply)

  implicit val tableMetricsJsonFormat: RootJsonFormat[TableMetrics] = jsonFormat7(TableMetrics.apply)

  implicit val typeAsEnumJsonFormat: RootJsonFormat[TypeAsEnum] = jsonFormat2(TypeAsEnum.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Result Set Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit val queryResultJsonFormat: RootJsonFormat[QueryResult] = jsonFormat8(QueryResult.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Row/Field Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit object ColumnTypeJsonFormat extends JsonFormat[ColumnType] {
    override def read(json: JsValue): ColumnType = ColumnTypes.withName(json.convertTo[String])

    override def write(columnType: ColumnType): JsValue = JsString(columnType.toString)
  }

  implicit val fieldMetadataJsonFormat: RootJsonFormat[FieldMetadata] = jsonFormat4(FieldMetadata.apply)

  implicit val fieldJsonFormat: RootJsonFormat[Field] = jsonFormat3(Field.apply)

  implicit object FieldSeqJsonFormat extends JsonFormat[Seq[Field]] {
    override def read(json: JsValue): Seq[Field] = json match {
      case JsArray(values) => values.map(_.convertTo[Field])
    }

    override def write(fields: Seq[Field]): JsValue = JsArray(fields.map(_.toJson): _*)
  }

  implicit object RowMetadataJsonFormat extends JsonFormat[RowMetadata] {
    override def read(json: JsValue): RowMetadata = json match {
      case JsNumber(value) => RowMetadata.decode(value.toByte)
    }

    override def write(rmd: RowMetadata): JsValue = JsNumber(rmd.encode.toInt)
  }

  implicit val rowJsonFormat: RootJsonFormat[Row] = jsonFormat3(Row.apply)

  def unwrap(jsValue: JsValue): Any = jsValue match {
    case js: JsArray => js.elements map unwrap
    case JsNull => null
    case JsBoolean(value) => value
    case JsNumber(value) => value
    case js: JsObject => js.fields map { case (name, jsValue) => name -> unwrap(jsValue) }
    case JsString(value) => value
    case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
  }

  ////////////////////////////////////////////////////////////////////////
  //      TupleSet Implicits
  ////////////////////////////////////////////////////////////////////////

  implicit object TupleSetJsonFormat extends JsonFormat[TupleSet] {
    override def read(jsValue: JsValue): TupleSet = jsValue match {
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON }
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

  implicit object TupleSetSeqJsonFormat extends JsonFormat[Seq[TupleSet]] {
    override def read(value: JsValue): Seq[TupleSet] = ???

    override def write(items: Seq[TupleSet]): JsValue = JsArray(items.map(_.toJson):_*)
  }

}

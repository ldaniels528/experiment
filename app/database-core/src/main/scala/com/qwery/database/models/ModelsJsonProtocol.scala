package com.qwery.database
package models

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.models.{ColumnSpec, EntityRef, Table, TableIndex, TypeAsEnum}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.{models => mx}
import spray.json._

import java.util.{Date, UUID}

/**
  * Models JSON Protocol
  */
object ModelsJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  ////////////////////////////////////////////////////////////////////////
  //      Utility Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit object OptionAnyJsonFormat extends JsonFormat[Option[Any]] {
    override def read(json: JsValue): Option[Any] = Option(json.unwrapJSON)

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

  final implicit object SeqSeqOptionAnyJsonFormat extends JsonFormat[Seq[Seq[Option[Any]]]] {
    override def read(json: JsValue): Seq[Seq[Option[Any]]] = json match {
      case JsArray(rowsJs) => rowsJs collect {
        case JsArray(colsJs) => colsJs.map(v => Option(v.unwrapJSON))
      }
    }

    override def write(rows: Seq[Seq[Option[Any]]]): JsValue = {
      JsArray((for {row <- rows; array = JsArray(row.map(_.toJson): _*)} yield array): _*)
    }
  }

  ////////////////////////////////////////////////////////////////////////
  //      Core Model Custom Implicits
  ////////////////////////////////////////////////////////////////////////

  case class TableRefJs(databaseName: String, schemaName: String, tableName: String)

  final implicit val tableRefJsJsonFormat: RootJsonFormat[TableRefJs] = jsonFormat3(TableRefJs.apply)

  final implicit object TableRefJsonFormat extends JsonFormat[EntityRef] {
    override def read(json: JsValue): EntityRef = {
      val js = json.convertTo[TableRefJs]
      new EntityRef(js.databaseName, js.schemaName, js.tableName)
    }

    override def write(ref: EntityRef): JsValue = TableRefJs(
      databaseName = ref.databaseName || DEFAULT_DATABASE,
      schemaName = ref.schemaName || DEFAULT_SCHEMA,
      tableName = ref.name).toJson
  }

  ////////////////////////////////////////////////////////////////////////
  //      Core Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val columnSpecJsonFormat: RootJsonFormat[ColumnSpec] = jsonFormat2(ColumnSpec.apply)

  final implicit val columnJsonFormat: RootJsonFormat[mx.Column] = jsonFormat5(mx.Column.apply)

  final implicit val tableIndexJsonFormat: RootJsonFormat[TableIndex] = jsonFormat4(TableIndex.apply)

  final implicit val tableJsonFormat: RootJsonFormat[Table] = jsonFormat5(Table.apply)

  final implicit val typeAsEnumJsonFormat: RootJsonFormat[TypeAsEnum] = jsonFormat2(TypeAsEnum.apply)

}

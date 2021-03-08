package com.qwery.database
package models

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.qwery.database.models.TableConfig.{ExternalTableConfig, PhysicalTableConfig, VirtualTableConfig}
import com.qwery.implicits.MagicImplicits
import com.qwery.models.{ColumnSpec, EntityRef, Table, TableIndex, TypeAsEnum}
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

  case class EntityRefJs(databaseName: Option[String], schemaName: Option[String], tableName: String)

  final implicit val entityRefJsJsonFormat: RootJsonFormat[EntityRefJs] = jsonFormat3(EntityRefJs.apply)

  final implicit object EntityRefJsonFormat extends JsonFormat[EntityRef] {
    override def read(json: JsValue): EntityRef = {
      json.convertTo[EntityRefJs] as { js => EntityRef(js.databaseName, js.schemaName, js.tableName) }
    }

    override def write(ref: EntityRef): JsValue = {
      EntityRefJs(databaseName = ref.databaseName, schemaName = ref.schemaName, tableName = ref.name).toJson
    }
  }

  ////////////////////////////////////////////////////////////////////////
  //      Core Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val columnSpecJsonFormat: RootJsonFormat[ColumnSpec] = jsonFormat2(ColumnSpec.apply)

  final implicit val columnModelJsonFormat: RootJsonFormat[mx.Column] = jsonFormat6(mx.Column.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Column Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val columnMetadataJsonFormat: RootJsonFormat[ColumnMetadata] = jsonFormat7(ColumnMetadata.apply)

  final implicit val columnJsonFormat: RootJsonFormat[Column] = jsonFormat6(Column.apply)

  final implicit val databaseConfigJsonFormat: RootJsonFormat[DatabaseConfig] = jsonFormat1(DatabaseConfig.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Field Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val fieldMetadataJsonFormat: RootJsonFormat[FieldMetadata] = jsonFormat4(FieldMetadata.apply)

  final implicit val fieldJsonFormat: RootJsonFormat[Field] = jsonFormat3(Field.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Table Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val typeAsEnumJsonFormat: RootJsonFormat[TypeAsEnum] = jsonFormat2(TypeAsEnum.apply)

  final implicit val tableJsonFormat: RootJsonFormat[Table] = jsonFormat5(Table.apply)

  final implicit val tableIndexJsonFormat: RootJsonFormat[TableIndex] = jsonFormat4(TableIndex.apply)

  final implicit val externalTableConfigJsonFormat: RootJsonFormat[ExternalTableConfig] = jsonFormat6(ExternalTableConfig.apply)

  final implicit val physicalTableConfigJsonFormat: RootJsonFormat[PhysicalTableConfig] = jsonFormat1(PhysicalTableConfig.apply)

  final implicit val virtualTableConfigJsonFormat: RootJsonFormat[VirtualTableConfig] = jsonFormat1(VirtualTableConfig.apply)

  final implicit val tableConfigJsonFormat: RootJsonFormat[TableConfig] = jsonFormat6(TableConfig.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Metrics Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val loadMetricsJsonFormat: RootJsonFormat[LoadMetrics] = jsonFormat3(LoadMetrics.apply)

  final implicit val tableMetricsJsonFormat: RootJsonFormat[TableMetrics] = jsonFormat5(TableMetrics.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Summary Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val tableSummaryJsonFormat: RootJsonFormat[TableSummary] = jsonFormat7(TableSummary.apply)

  final implicit val databaseSummaryJsonFormat: RootJsonFormat[DatabaseSummary] = jsonFormat2(DatabaseSummary.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Search Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val columnSearchResultJsonFormat: RootJsonFormat[ColumnSearchResult] = jsonFormat4(ColumnSearchResult.apply)

  final implicit val databaseSearchResultJsonFormat: RootJsonFormat[DatabaseSearchResult] = jsonFormat1(DatabaseSearchResult.apply)

  final implicit val schemaSearchResultJsonFormat: RootJsonFormat[SchemaSearchResult] = jsonFormat2(SchemaSearchResult.apply)

  final implicit val tableSearchResultJsonFormat: RootJsonFormat[TableSearchResult] = jsonFormat5(TableSearchResult.apply)

  ////////////////////////////////////////////////////////////////////////
  //      Result Set Model Implicits
  ////////////////////////////////////////////////////////////////////////

  final implicit val queryResultJsonFormat: RootJsonFormat[QueryResult] = jsonFormat5(QueryResult.apply)

  final implicit val rowJsonFormat: RootJsonFormat[Row] = jsonFormat3(Row.apply)

  final implicit val rowStatisticsJsonFormat: RootJsonFormat[RowStatistics] = jsonFormat6(RowStatistics.apply)

  final implicit val updateCount: RootJsonFormat[UpdateCount] = jsonFormat2(UpdateCount.apply)

}

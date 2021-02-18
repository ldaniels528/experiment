package com.qwery.database

import com.qwery.database.types.QxAny

/**
 * Represents a field
 * @param name       the name of the field
 * @param metadata   the [[FieldMetadata field metadata]] containing the field type and other useful information
 * @param typedValue the [[QxAny value]] of the field
 */
case class Field(name: String, metadata: FieldMetadata, typedValue: QxAny) {

  def value: Option[Any] = typedValue.value

}

object Field  {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val fieldJsonFormat: RootJsonFormat[Field] = jsonFormat3(Field.apply)

  implicit object fieldSeqJsonFormat extends JsonFormat[Seq[Field]] {
    override def read(json: JsValue): Seq[Field] = json match {
      case JsArray(values) => values.map(_.convertTo[Field])
    }

    override def write(fields: Seq[Field]): JsValue = JsArray(fields.map(_.toJson): _*)
  }

}
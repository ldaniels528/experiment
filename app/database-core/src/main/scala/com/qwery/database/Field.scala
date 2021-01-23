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
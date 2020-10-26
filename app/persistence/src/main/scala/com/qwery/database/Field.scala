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

/**
 * Field Companion
 */
object Field {

  /**
   * Creates a new field
   * @param name     the name of the field
   * @param metadata the [[FieldMetadata field metadata]] containing the field type and other useful information
   * @param value    the value of the field
   */
  def create(name: String, metadata: FieldMetadata, value: Any) = new Field(name, metadata, QxAny(value))

}
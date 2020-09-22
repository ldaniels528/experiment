package com.qwery.database

/**
 * Represents a field
 * @param name     the name of the field
 * @param metadata the [[FieldMetadata field metadata]] containing the field type and other useful information
 * @param value    the value of the field
 */
case class Field(name: String, metadata: FieldMetadata, value: Option[Any])

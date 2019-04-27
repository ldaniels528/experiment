package com.qwery.models

import com.qwery.models.expressions.LocalVariableRef

/**
  * Represents a location of a data set (e.g. table or file)
  * @author lawrence.daniels@gmail.com
  */
sealed trait Location extends Queryable with Aliasable {
  def description: String
}

/**
  * Represents the location path (e.g. '/securities/nasdaq')
  * @param path the given location path
  */
case class LocationRef(path: String) extends Location {
  override val description = s"Location $path"
}

/**
  * Represents a reference to a [[Table table]]
  * @param name the name of the table
  */
case class TableRef(name: String) extends Location {
  override val description = s"Table $name"
}

/**
  * Represents the variable location path (e.g. '@s3Path')
  * @param variable the given [[LocalVariableRef local variable]]
  */
case class VariableLocationRef(variable: LocalVariableRef) extends Location {
  override val description = s"Location $variable"
}
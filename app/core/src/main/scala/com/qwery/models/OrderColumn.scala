package com.qwery.models

/**
  * Represents an Ordered Column
  * @param name        the name of the column
  * @param isAscending indicates whether the column is ascending (or conversly descending)
  */
case class OrderColumn(name: String, isAscending: Boolean) extends Aliasable {

  def asc: OrderColumn = this.copy(isAscending = true)

  def desc: OrderColumn = this.copy(isAscending = false)

}
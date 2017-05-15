package com.github.ldaniels528.qwery.ops

/**
  * Represents an ordered column
  * @param name the name of the column
  * @param ascending indicates the sorting direction (ascending when true, descending if false)
  */
case class OrderedColumn(name: String, ascending: Boolean) extends SQLLike {
  override def toSQL = s"${if (name.contains(' ')) s"`$name`" else name} ${if(ascending) "ASC" else "DESC"}"
}

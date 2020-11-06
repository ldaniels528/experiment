package com.qwery.database

/**
 * Represents a row; a collection of tuples
 */
case class TupleSet(items: (String, Any)*) {

  def ++(that: TupleSet): TupleSet = TupleSet(this.toMap ++ that.toMap)

  def exists(f: ((String, Any)) => Boolean): Boolean = items.exists(f)

  def forall(f: ((String, Any)) => Boolean): Boolean = items.forall(f)

  def foreach(f: ((String, Any)) => Unit): Unit = items.foreach(f)

  def get(name: String): Option[Any] = toMap.get(name)

  def keys: Seq[String] = items.map(_._1)

  def isEmpty: Boolean = items.isEmpty

  def nonEmpty: Boolean = items.nonEmpty

  def toList: List[(String, Any)] = items.toList

  def toMap: Map[String, Any] = Map(items: _*)

  def toSeq: Seq[(String, Any)] = items

  override def toString: String = toMap.toString

  def values: Seq[Any] = items.map(_._2)

}

/**
 * TupleSet Companion
 */
object TupleSet {
  def apply(mapping: Map[String, Any]) = new TupleSet(mapping.toSeq: _*)

  //implicit def map2TupleSet(mapping: Map[String, Any]): TupleSet = TupleSet(mapping)

  //implicit def tuples2TupleSet(items: Seq[(String, Any)]): TupleSet = TupleSet(items: _*)

}
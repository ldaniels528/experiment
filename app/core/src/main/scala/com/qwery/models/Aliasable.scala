package com.qwery.models

/**
  * Represents an "aliasable" entity; one that can be renamed
  * @author lawrence.daniels@gmail.com
  */
trait Aliasable {
  private var _alias: Option[String] = None

  def alias: Option[String] = _alias

  def as(alias: String): this.type = {
    this._alias = Option(alias)
    this
  }

  def as(alias: Option[String]): this.type = {
    this._alias = alias
    this
  }

}

/**
  * Aliasable Companion
  * @author lawrence.daniels@gmail.com
  */
object Aliasable {

  /**
    * Aliasing Enrichment
    * @param invokable the [[Invokable]]
    */
  final implicit class AliasEnrichment[T <: Invokable](val invokable: T) extends AnyVal {
    @inline def as(alias: String): T = invokable match {
      case aliasable: Aliasable => aliasable.as(alias).asInstanceOf[T]
      case _ => throw new IllegalArgumentException(s"Instance type '${invokable.getClass.getSimpleName}' does not support aliases")
    }
  }

}
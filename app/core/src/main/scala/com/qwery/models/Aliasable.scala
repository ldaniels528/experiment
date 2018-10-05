package com.qwery.models

import com.qwery.models.expressions.Expression

/**
  * Represents an "aliasable" entity; one that can be renamed
  * @author lawrence.daniels@gmail.com
  */
trait Aliasable {
  private var _alias: Option[String] = None

  def alias: Option[String] = _alias

  def as(alias: String): this.type = as(alias = Option(alias))

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
    * Expression Aliases
    * @param expression the given [[Expression]] to alias
    */
  final implicit class ExpressionAliases[T <: Expression](val expression: T) extends AnyVal {
    @inline def as(alias: String): T = expression match {
      case aliasable: Aliasable => aliasable.as(alias).asInstanceOf[T]
      case _ => die(s"Instance type '${expression.getClass.getSimpleName}' does not support aliases")
    }
  }

  /**
    * Invokable Aliases
    * @param invokable the given [[Invokable]] to alias
    */
  final implicit class InvokableAliases[T <: Invokable](val invokable: T) extends AnyVal {
    @inline def as(alias: String): T = invokable match {
      case aliasable: Aliasable => aliasable.as(alias).asInstanceOf[T]
      case _ => die(s"Instance type '${invokable.getClass.getSimpleName}' does not support aliases")
    }
  }

}
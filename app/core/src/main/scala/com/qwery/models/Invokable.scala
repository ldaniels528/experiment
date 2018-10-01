package com.qwery.models

import com.qwery.models.expressions.VariableRef

/**
  * Represents an executable operation
  * @author lawrence.daniels@gmail.com
  */
trait Invokable

/**
  * Invokable Companion
  * @author lawrence.daniels@gmail.com
  */
object Invokable {

  /**
    * Invokable Enrichment
    * @param invokable the given [[Invokable]]
    */
  final implicit class InvokableEnriched(val invokable: Invokable) extends AnyVal {

    @inline def isQuery: Boolean = invokable match {
      case _: ProcedureCall => true
      case _: Select => true
      case _: Union => true
      case _ => false
    }

    @inline def isVariable: Boolean = invokable match {
      case _: VariableRef => true
      case _ => false
    }
  }

}
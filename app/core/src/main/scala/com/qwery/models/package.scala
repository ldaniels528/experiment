package com.qwery

import com.qwery.models.expressions._

/**
  * models package object
  * @author lawrence.daniels@gmail.com
  */
package object models {

  /**
    * Shortcut for creating a local variable reference
    * @param name the name of the variable
    * @return a new [[LocalVariableRef local variable reference]]
    */
  def $(name: String) = LocalVariableRef(name)

  /**
    * Shortcut for creating a row-set variable reference
    * @param name the name of the variable
    * @return a new [[RowSetVariableRef row-set variable reference]]
    */
  def @@(name: String) = RowSetVariableRef(name)

  /**
    * Expression Enrichment
    * @param expression the given [[Expression expression]]
    */
  final implicit class ExpressionEnrichment(val expression: Expression) extends AnyVal {

    /**
      * Creates an BETWEEN clause
      * @param from the from expression
      * @param to the to expression
      * @return a [[BETWEEN]] expression
      */
    @inline def between(from: Expression, to: Expression): BETWEEN = BETWEEN(expr = expression, from, to)

    /**
      * Creates an OVER clause
      * @param partitionBy the given partition by columns
      * @param orderBy     the given order by columns
      * @param range       the RANGE BETWEEN clause
      * @return a [[Over window]] expression
      */
    @inline def over(partitionBy: Seq[Field] = Nil,
                     orderBy: Seq[OrderColumn] = Nil,
                     range: Option[Condition] = None): Over = {
      Over(
        expression = expression,
        partitionBy = partitionBy,
        orderBy = orderBy,
        range = range
      )
    }
  }

  /**
    * Invokable Conversions
    * @param invokable the given [[Invokable invokable]]
    */
  final implicit class InvokableConversions(val invokable: Invokable) extends AnyVal {
    @inline def toQueryable: Queryable = invokable match {
      case q: Queryable => q
      case x => throw new IllegalArgumentException(s"Invalid input source '$x'")
    }
  }

}

package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.{Executable, Expression, NamedExpression, ResultSet, Scope}

import scala.language.postfixOps

/**
  * Represents a collection of insert values
  * @author lawrence.daniels@gmail.com
  */
case class InsertValues(dataSets: Seq[Seq[Expression]]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val fieldNames = (1 to dataSets.headOption.map(_.size).getOrElse(0)) map (_ => NamedExpression.randomName())
    ResultSet(rows = dataSets map { dataSet =>
      fieldNames zip dataSet map { case (field, value) =>
        field -> value.evaluate(scope).orNull
      }
    } toIterator)
  }

}
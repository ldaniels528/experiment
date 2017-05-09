package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ResultSet

import scala.collection.Iterable

/**
  * Represents a collection of insert values
  * @author lawrence.daniels@gmail.com
  */
case class InsertValues(fields: Seq[Field], dataSets: Iterable[Seq[Any]]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    dataSets map { dataSet =>
      fields zip dataSet map { case (field, value) =>
        field.name -> value
      }
    }
  }

}
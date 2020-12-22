package com.qwery.database

import com.qwery.models.expressions.Expression

/**
  * System-defined functions
  */
package object functions {

  /**
    * System-defined aggregation functions
    */
  val aggregateFunctions: Map[String, (String, List[Expression]) => AggregateFunction] = Map(
    "avg" -> Avg,
    "count" -> Count,
    "distinct" -> Distinct,
    "max" -> Max,
    "min" -> Min,
    "sum" -> Sum
  )

  /**
    * System-defined transformation functions
    */
  val transformationFunctions: Map[String, (String, List[Expression]) => TransformationFunction] = Map(
    "round" -> Round
  )

  /**
    * All system-defined functions
    */
  val builtinFunctions: Map[String, (String, List[Expression]) => Function] = aggregateFunctions ++ transformationFunctions

}

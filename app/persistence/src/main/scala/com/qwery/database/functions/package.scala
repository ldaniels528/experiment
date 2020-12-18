package com.qwery.database

import com.qwery.models.expressions.Expression

package object functions {

  val simpleFunctions: Map[String, (String, List[Expression]) => Function] = Map(
    "avg" -> Avg,
    "count" -> Count,
    "distinct" -> Distinct,
    "max" -> Max,
    "min" -> Min,
    "sum" -> Sum
  )

  val aggregateFunctions: Map[String, (String, List[Expression]) => AggregateFunction] = Map(
    "avg" -> Avg,
    "count" -> Count,
    "distinct" -> Distinct,
    "max" -> Max,
    "min" -> Min,
    "sum" -> Sum
  )

}

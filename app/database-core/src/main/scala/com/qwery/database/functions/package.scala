package com.qwery.database

import com.qwery.models.expressions.Expression

/**
  * System-defined functions
  */
package object functions {

  /**
    * System-defined aggregation functions
    */
 private val aggregateFunctions: Map[String, (String, List[Expression]) => AggregateFunction] = Map(
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
  private val transformationFunctions: Map[String, (String, List[Expression]) => TransformationFunction] = Map(
    "count" -> Count,
    "now" -> Now,
    "round" -> Round
  )

  /**
   * All system-defined functions
   */
  private val builtinFunctions: Map[String, (String, List[Expression]) => Function] = aggregateFunctions ++ transformationFunctions

  val tempName: Any => String = (_: Any) => java.lang.Long.toString(System.nanoTime(), 36)

  def isAggregateFunction(functionName: String): Boolean = aggregateFunctions.contains(functionName)

  def lookupAggregationFunction(functionName: String): (String, List[Expression]) => AggregateFunction = {
    aggregateFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

  def isBuiltinFunction(functionName: String): Boolean = builtinFunctions.contains(functionName)

  def lookupBuiltinFunction(functionName: String): (String, List[Expression]) => Function = {
    builtinFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

  def lookupTransformationFunction(functionName: String): (String, List[Expression]) => TransformationFunction = {
    transformationFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

}

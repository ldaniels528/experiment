package com.qwery.database

import com.qwery.models.expressions.Expression

/**
  * System-defined functions
  */
package object functions {

  //////////////////////////////////////////////////////////////////
  //      AGGREGATION / SUMMARIZATION
  //////////////////////////////////////////////////////////////////

  /**
   * System-defined aggregation functions
   */
  private val aggregateFunctions: Map[String, (String, List[Expression]) => AggregateFunction] = Map(
    "avg" -> Avg,
    "count" -> Count,
    "countdistinct" -> CountDistinct,
    "distinct" -> Distinct,
    "max" -> Max,
    "min" -> Min,
    "sum" -> Sum
  )

  def isAggregateFunction(functionName: String): Boolean = aggregateFunctions.contains(functionName)

  def isSummarizationFunction(functionName: String): Boolean = isAggregateFunction(functionName)

  def lookupAggregationFunction(functionName: String): (String, List[Expression]) => AggregateFunction = {
    aggregateFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

  //////////////////////////////////////////////////////////////////
  //      TRANSFORMATION
  //////////////////////////////////////////////////////////////////

  /**
   * System-defined transformation functions
   */
  private val transformationFunctions: Map[String, (String, List[Expression]) => TransformationFunction] = Map(
    "count" -> Count,
    "now" -> Now,
    "round" -> Round,
    "version" -> Version
  )

  def isTransformationFunction(functionName: String): Boolean = transformationFunctions.contains(functionName)

  def lookupTransformationFunction(functionName: String): (String, List[Expression]) => TransformationFunction = {
    transformationFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

  //////////////////////////////////////////////////////////////////
  //      ALL BUILT-IN FUNCTIONS
  //////////////////////////////////////////////////////////////////

  /**
   * All system-defined functions
   */
  private val builtinFunctions: Map[String, (String, List[Expression]) => Function] = {
    aggregateFunctions ++ transformationFunctions
  }

  def isBuiltinFunction(functionName: String): Boolean = builtinFunctions.contains(functionName)

  def lookupBuiltinFunction(functionName: String): (String, List[Expression]) => Function = {
    builtinFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
  }

}

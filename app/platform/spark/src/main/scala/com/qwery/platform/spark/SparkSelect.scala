package com.qwery.platform.spark

import com.qwery.models.JoinTypes.JoinType
import com.qwery.models._
import com.qwery.models.expressions.NamedExpression._
import com.qwery.models.expressions._
import com.qwery.platform.spark.SparkQweryCompiler.Implicits._
import com.qwery.platform.spark.SparkSelect.SparkJoin
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Column => SparkColumn}
import org.slf4j.LoggerFactory

/**
  * Select Operation for Spark
  * @author lawrence.daniels@gmail.com
  */
case class SparkSelect(fields: Seq[Expression],
                       from: Option[SparkInvokable],
                       joins: Seq[SparkJoin],
                       groupBy: Seq[String],
                       orderBy: Seq[OrderColumn],
                       where: Option[Condition],
                       limit: Option[Int],
                       alias: Option[String])
  extends SparkInvokable {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: SparkQweryContext): Option[DataFrame] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._

    // get the data frame to use for selection
    val inputDf = from.flatMap(_.execute(input)) orFail "No input data"

    // capture the data frame's columns BEFORE aggregation and/or appending constants
    val dfColumns = inputDf.columns

    // build the Spark equivalent of the query
    val pipeline = List(
      processJoins _,
      processGroupBy _,
      processConstants _,
      processOrderBy _,
      processAliases _,
      processRowFiltering _,
      { df: DataFrame => processColumnFiltering(df, dfColumns) },
      processLimit _
    )
    pipeline.foldLeft(inputDf) { (df0, f) => f(df0) }
  }

  /**
    * Decodes the given aggregate field (likely a function) into its Spark counterpart.
    * @param aggField the given [[Field field]]
    * @return a tuple containing the field name and Spark function identifier (e.g. "avg")
    */
  def decode(aggField: Field): (String, String) = aggField match {
    case fx: SQLFunction1 => fx.field.getName -> fx.name
    case unknown =>
      throw new IllegalArgumentException(s"Unhandled aggregate expression $unknown")
  }

  /**
    * Reorder (and rename via aliases) the remaining fields within the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processAliases(df0: DataFrame): DataFrame = fields.foldLeft(df0) {
    case (df, fx: SQLFunction) => fx.alias.map(alias => df.withColumnRenamed(fx.toString, alias)) || df
    case (df, _) => df
  }

  /**
    * Removes all unselected columns from the data frame
    * @param df0       the given initial [[DataFrame]]
    * @param dfColumns the "virgin" column names; before the data was modified by aggregation or appending constants
    * @return the resultant [[DataFrame]]
    */
  def processColumnFiltering(df0: DataFrame, dfColumns: Seq[String]): DataFrame = {
    val selectedColumns = fields.map(_.getName)
    val columnsToRemove = dfColumns.filterNot(selectedColumns.contains)
    logger.info(s"selectedColumns = $selectedColumns, columnsToRemove = $columnsToRemove")
    //columnsToRemove.foldLeft(df0) { (df, col) => df.drop(col) }
    df0
  }

  /**
    * Append any constants (if present) to the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processConstants(df0: DataFrame)(implicit rc: SparkQweryContext): DataFrame = {
    import SparkQweryCompiler.Implicits._
    fields.collect { case cf: ConstantField => cf }.foldLeft(df0) {
      case (df, ref@ConstantField(value)) => df.withColumn(ref.name, value.compile)
    }
  }

  /**
    * Performs group by/aggregates the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processGroupBy(df0: DataFrame): DataFrame = if (groupBy.isEmpty) df0 else {
    val fxTuples = fields.collect { case f: SQLFunction1 if f.isAggregate => decode(f) }
    df0.groupBy(groupBy.map(f => col(f)): _*).agg(fxTuples.head, fxTuples.tail: _*)
  }

  /**
    * Perform join operations linking other datasets with the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processJoins(df0: DataFrame)(implicit rc: SparkQweryContext): DataFrame = {
    joins.foldLeft(df0) { case (dfA, SparkJoin(source, condition, kind)) =>
      logger.info(s"condition: $condition, kind: $kind")
      source.execute(input = None) map { dfB => dfA.join(dfB, condition, kind.toString.toLowerCase) } getOrElse dfA
    }
  }

  /**
    * Limits the size (in rows) of the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processLimit(df0: DataFrame): DataFrame = limit.map(df0.limit) || df0

  /**
    * Sorts the data frame by the order columns within the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processOrderBy(df0: DataFrame): DataFrame = if (orderBy.isEmpty) df0 else df0.orderBy(orderBy.map {
    case ref@OrderColumn(name, ascend) => if (ascend) asc(ref.alias || name) else desc(ref.alias || name)
  }: _*)

  /**
    * Performs filtering at the row-level
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processRowFiltering(df0: DataFrame)(implicit rc: SparkQweryContext): DataFrame =
    where.map(cond => df0.filter(cond.compile)) || df0

}

/**
  * SelectSpark Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkSelect {

  /**
    * Represents a JOIN operation
    * @param source    the given [[SparkInvokable source]]
    * @param condition the given [[SparkColumn condition]]
    * @param `type`    the given [[JoinType]]
    */
  case class SparkJoin(source: SparkInvokable, condition: SparkColumn, `type`: JoinType)

}
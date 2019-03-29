package com.qwery.platform.sparksql.embedded

import com.qwery.models.Aliasable
import com.qwery.models.JoinTypes.JoinType
import com.qwery.models.expressions.NamedExpression._
import com.qwery.models.expressions._
import com.qwery.platform.sparksql.embedded.SparkSelect.SparkJoin
import com.qwery.util.OptionHelper._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column => SparkColumn}
import org.slf4j.LoggerFactory

/**
  * Select Operation for Spark
  * @author lawrence.daniels@gmail.com
  */
case class SparkSelect(fields: Seq[(SparkColumn, Boolean, Option[String])],
                       from: Option[SparkInvokable],
                       joins: Seq[SparkJoin],
                       groupBy: Seq[SparkColumn],
                       orderBy: Seq[SparkColumn],
                       where: Option[Condition],
                       limit: Option[Int])
  extends SparkInvokable with Aliasable {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
    from.flatMap(_.execute(input)) map { df => pipeline.foldLeft[DataFrame](df) { (df0, f) => f(df0) } }
  }

  // build the Spark equivalent of the query
  private def pipeline(implicit rc: EmbeddedSparkContext): Seq[DataFrame => DataFrame] = Seq(
    processJoins,
    processGroupBy,
    processOrderBy,
    processRowFiltering,
    processColumnFiltering,
    processLimit
  )

  /**
    * Decodes the given aggregate field (likely a function) into its Spark counterpart.
    * @param aggField the given [[NamedExpression field]]
    * @return a tuple containing the field name and Spark function identifier (e.g. "avg")
    */
  def decode(aggField: NamedExpression): (String, String) = aggField match {
    case fx: SQLFunction1 => fx.field.getName -> fx.name
    case unknown => die(s"Unhandled aggregate expression $unknown")
  }

  /**
    * Removes all unselected columns from the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processColumnFiltering(df0: DataFrame)(implicit rc: EmbeddedSparkContext): DataFrame =
    df0.select(fields.collect {
      case (expr, false, _) => expr
      case (expr, true, anAlias) if anAlias.nonEmpty => anAlias.map(col) getOrElse expr
    }: _*)

  /**
    * Performs group by/aggregates the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processGroupBy(df0: DataFrame): DataFrame = if (groupBy.isEmpty) df0 else {
    val fxTuples = fields.collect { case (fx, true, _) => fx }
    LoggerFactory.getLogger(getClass).info(s"fxTuples => $fxTuples")
    if (fxTuples.nonEmpty) df0.groupBy(groupBy: _*).agg(fxTuples.head, fxTuples.tail: _*) else df0
  }

  /**
    * Perform join operations linking other datasets with the data frame
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processJoins(df0: DataFrame)(implicit rc: EmbeddedSparkContext): DataFrame = {
    import SparkAliasable._

    joins.foldLeft(df0) { case (dfA, SparkJoin(source, condition, joinType)) =>
      source.execute(input = None) map { dfB =>
        logger.info("*" * 80)
        logger.info(s"dfA: ${alias.orNull}")
        logger.info(s"dfB: ${source.getAlias.orNull}")

        dfA.show(5)
        dfB.show(5)

        val dfA0 = alias.map(dfA.as) getOrElse dfA
        val dfB0 = source.getAlias.map(alias => dfB.as(alias)) getOrElse dfB

        dfA0.join(dfB0, condition, joinType.toString.toLowerCase)
      } getOrElse dfA
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
  def processOrderBy(df0: DataFrame): DataFrame = if (orderBy.isEmpty) df0 else df0.orderBy(orderBy: _*)

  /**
    * Performs filtering at the row-level
    * @param df0 the given initial [[DataFrame]]
    * @return the resultant [[DataFrame]]
    */
  def processRowFiltering(df0: DataFrame)(implicit rc: EmbeddedSparkContext): DataFrame = {
    import SparkEmbeddedCompiler.Implicits._
    where.map(_.compile).map(df0.filter) || df0
  }

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

  /**
    * Represents a UNION operation
    * @param query0     the first query
    * @param query1     the second query
    * @param isDistinct indicates whether the resulting records should be a distinct set.
    */
  case class SparkUnion(query0: SparkInvokable,
                        query1: SparkInvokable,
                        isDistinct: Boolean) extends SparkInvokable with Aliasable {
    override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = {
      val df = for {
        df0 <- query0.execute(input)
        df1 <- query1.execute(input)
      } yield df0 union df1

      if (isDistinct) df.map(_.distinct()) else df
    }
  }

}
package com.qwery.database

import java.util.concurrent.atomic.AtomicInteger

import com.qwery.database.device.BlockDevice
import com.qwery.database.functions._
import com.qwery.database.types.QxAny
import com.qwery.models.OrderColumn
import com.qwery.models.expressions.{AllFields, BasicField, Expression, FunctionCall, Distinct => SQLDistinct, Field => SQLField}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._

import scala.collection.mutable
import scala.language.postfixOps

/**
 * Table Query
 * @param tableDevice the [[BlockDevice table device]] to query
 */
class TableQuery(tableDevice: BlockDevice) {
  private val tempName = () => java.lang.Long.toString(System.currentTimeMillis(), 36)

  /**
    * Executes aggregation, summarization and transformation queries
    * @param projection the [[Expression field projection]]
    * @param where      the condition which determines which records are included
    * @param groupBy    the optional aggregation columns
    * @param orderBy    the columns to order by
    * @param limit      the optional limit
    * @return a [[BlockDevice device]] containing the rows
    */
  def select(projection: Seq[Expression],
             where: KeyValues,
             groupBy: Seq[SQLField] = Nil,
             orderBy: Seq[OrderColumn] = Nil,
             limit: Option[Int] = None): BlockDevice = {
    if (groupBy.nonEmpty) aggregationQuery(projection, where, groupBy, orderBy, limit)
    else if (isSummarization(projection)) summarizationQuery(projection, where, orderBy, limit)
    else transformationQuery(projection, where, orderBy, limit)
  }

  //////////////////////////////////////////////////////////////////
  //      AGGREGATION
  //////////////////////////////////////////////////////////////////

  /**
    * Executes an aggregation query
    * @param projection the [[Expression field projection]]
    * @param where      the [[KeyValues inclusion criteria]]
    * @param groupBy    the columns to group by
    * @param orderBy    the columns to order by
    * @param limit      the maximum number of rows for which to return
    * @return a [[BlockDevice device]] containing the rows
    */
  def aggregationQuery(projection: Seq[Expression],
                       where: KeyValues,
                       groupBy: Seq[SQLField],
                       orderBy: Seq[OrderColumn],
                       limit: Option[Int]): BlockDevice = {
    // determine the projection, reference and group by columns
    val projectionColumns: Seq[Column] = getProjectionColumns(projection)
    val referenceColumns: Seq[Column] = getReferencedColumns(projection)
    val referenceColumnNames: Set[String] = referenceColumns.map(_.name).toSet
    val groupByColumns: Seq[Column] = getColumnsByName(groupBy.map(_.name))
    val groupByColumnNames: Seq[String] = groupByColumns.map(_.name)

    // partition the rows into temporary tables per grouped key
    val tempTables = mutable.Map[String, BlockDevice]()
    val counter = new AtomicInteger(0)
    tableDevice.whileKV(where) { srcKV =>
      val key = groupByColumnNames.flatMap(srcKV.get).mkString("\t")
      implicit val groupDevice: BlockDevice = tempTables.getOrElseUpdate(key, createTempTable(referenceColumns))
      val dstKV = srcKV.filter { case (name, _) => referenceColumnNames.contains(name) }
      groupDevice.writeRow(dstKV.toBinaryRow(groupDevice.length))
      limit.isEmpty || limit.exists(counter.addAndGet(1) < _)
    }

    // aggregate the results
    val results = createTempTable(projectionColumns)
    tempTables.values foreach { implicit groupDevice =>
      // compile the projection into aggregate expressions
      val aggExpressions: Seq[AggregateExpr] = getAggregateProjection(projection)
      // update the aggregate expressions
      groupDevice.use(_.foreachKVP(keyValues => aggExpressions.foreach(_ += keyValues)))
      // create the aggregate key-values
      val dstKV = KeyValues(aggExpressions map { expr => expr.name -> expr.collect }: _*)
      // write the aggregated key-values as a row
      results.writeRow(dstKV.toBinaryRow(rowID = results.length)(results))
    }

    // order the results?
    sortResults(results, orderBy)
  }

  private def getAggregateProjection(expressions: Seq[Expression]): Seq[AggregateExpr] = {
    expressions map {
      case AllFields => die("Aggregation function or constant value expected")
      case f: BasicField => AggregateField(name = f.alias || f.name, srcName = f.name)
      case fc@FunctionCall(functionName, List(SQLDistinct(args))) if functionName equalsIgnoreCase "count" =>
        CountDistinct(fc.alias || tempName(), args)
      case fc@FunctionCall(functionName, args) =>
        val fxTemplate = aggregateFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
        fxTemplate(fc.alias || tempName(), args)
      case expression => die(s"Unconverted expression: $expression")
    }
  }

  private def getColumnsByName(names: Seq[String]): Seq[Column] = {
    val projectionColumnsMap = Map(tableDevice.columns.map(c => c.name -> c): _*)
    (for {
      name <- names
      column = projectionColumnsMap.getOrElse(name, die(s"Column '$name' does not exist"))
    } yield column).distinct
  }

  /**
    * Returns all columns referenced within the expressions
    * @param expressions the collection of expressions
    * @return the referenced [[Column columns]]
    */
  private def getReferencedColumns(expressions: Seq[Expression]): Seq[Column] = {
    expressions flatMap {
      case AllFields => tableDevice.columns
      case f: BasicField => tableDevice.columns.find(_.name == f.name).toSeq
      case FunctionCall(functionName, List(SQLDistinct(args))) if functionName equalsIgnoreCase "count" =>
        getReferencedColumns(args.filterNot(_ == AllFields))
      case FunctionCall(_, args) => getReferencedColumns(args.filterNot(_ == AllFields))
      case expression => die(s"Unconverted expression: $expression")
    } distinct
  }

  //////////////////////////////////////////////////////////////////
  //      SUMMARIZATION
  //////////////////////////////////////////////////////////////////

  /**
    * Executes a summarization query
    * @param projection the [[Expression field projection]]
    * @param where      the [[KeyValues inclusion criteria]]
    * @param orderBy    the columns to order by
    * @param limit      the maximum number of rows for which to return
    * @return a [[BlockDevice device]] containing the rows
    */
  def summarizationQuery(projection: Seq[Expression],
                         where: KeyValues,
                         orderBy: Seq[OrderColumn],
                         limit: Option[Int]): BlockDevice = {
    // compile the projection into aggregate expressions
    val aggExpressions: Seq[AggregateExpr] = getAggregateProjection(projection)

    // update the aggregate expressions
    tableDevice.whileKV(where, limit) { srcKV => aggExpressions.foreach(_ += srcKV) }

    // create the aggregate key-values
    val dstKV = KeyValues(aggExpressions map { expr => expr.name -> expr.collect }: _*)

    // write the aggregated key-values as a row
    val projectionColumns: Seq[Column] = getProjectionColumns(projection)
    implicit val results: BlockDevice = createTempTable(projectionColumns, fixedRowCount = 1)
    results.writeRow(dstKV.toBinaryRow(rowID = results.length))
    sortResults(results, orderBy)
  }

  /**
    * Indicates whether the projection contains at least one aggregate function
    * @param projection the collection of projection [[Expression expressions]]
    * @return true, if the projection contains at least one aggregate function
    */
  def isSummarization(projection: Seq[Expression]): Boolean = {
    projection.exists {
      case FunctionCall(name, _) => aggregateFunctions.contains(name)
      case _ => false
    }
  }

  //////////////////////////////////////////////////////////////////
  //      TRANSFORMATION
  //////////////////////////////////////////////////////////////////

  /**
    * Executes a transformation query
    * @param projection the [[Expression field projection]]
    * @param where      the condition which determines which records are included
    * @param orderBy    the columns to order by
    * @param limit      the optional limit
    * @return a [[BlockDevice device]] containing the rows
    */
  def transformationQuery(projection: Seq[Expression],
                          where: KeyValues,
                          orderBy: Seq[OrderColumn],
                          limit: Option[Int]): BlockDevice = {
    // determine the projection columns
    val projectionColumns = getTransformationColumns(projection)

    // build the result set
    implicit val results: BlockDevice = createTempTable(projectionColumns)
    tableDevice.whileRow(where, limit) { srcRow =>
      val dstRow = Row(id = srcRow.id, metadata = srcRow.metadata, fields = getTransformationProjection(srcRow, projection))
      results.writeRow(dstRow.toBinaryRow(results.length))
    }

    // order the results?
    sortResults(results, orderBy)
  }

  private def getTransformationColumns(expressions: Seq[Expression]): Seq[Column] = {
    expressions flatMap {
      case AllFields => tableDevice.columns
      case f: BasicField => tableDevice.columns.find(_.name == f.name).map(_.copy(name = f.alias || f.name)).toSeq
      case fc@FunctionCall(functionName, args) =>
        val fxTemplate = transformationFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
        val fx = fxTemplate(fc.alias || tempName(), args)
        Seq(Column(name = fx.name, metadata = ColumnMetadata(`type` = fx.returnType)))
      case expression => die(s"Unconverted expression: $expression")
    } distinct
  }

  private def getTransformationProjection(srcRow: Row, expressions: Seq[Expression]): Seq[Field] = {
    expressions flatMap {
      case AllFields => srcRow.fields
      case f: BasicField => srcRow.fields.find(_.name == f.name).toSeq
      case fc@FunctionCall(functionName, args) =>
        val fxTemplate = transformationFunctions.getOrElse(functionName.toLowerCase, die(s"Function '$functionName' does not exist"))
        val fx = fxTemplate(fc.alias || tempName(), args)
        Seq(Field(name = fx.name, metadata = FieldMetadata(), QxAny(Option(fx.execute(srcRow.toKeyValues)))))
      case expression => die(s"Unconverted expression: $expression")
    }
  }

  //////////////////////////////////////////////////////////////////
  //      COMMON
  //////////////////////////////////////////////////////////////////

  private def getProjectionColumns(expressions: Seq[Expression]): Seq[Column] = {
    expressions flatMap {
      case AllFields => tableDevice.columns
      case f: BasicField => tableDevice.columns.find(_.name == f.name).map(_.copy(name = f.alias || f.name)).toSeq
      case fc@FunctionCall(_functionName, args) =>
        val functionName = _functionName.toLowerCase
        // is it a built-in function?
        if (builtinFunctions.contains(functionName)) {
          val fxTemplate = builtinFunctions(functionName)
          val fx = fxTemplate(fc.alias || tempName(), args)
          Seq(Column(name = fx.name, metadata = ColumnMetadata(`type` = fx.returnType)))
        }
        // is it a user-defined function?
        // TODO implement user-defined function
        // it's not a function ...
        else die(s"Function '$functionName' does not exist")
      case expression => die(s"Unconverted expression: $expression")
    } distinct
  }

  private def sortResults(results: BlockDevice, orderBy: Seq[OrderColumn]): BlockDevice = {
    if (orderBy.nonEmpty) {
      val orderByColumn = orderBy.headOption
      val sortColumnID = results.columns.indexWhere(col => orderByColumn.exists(o => o.alias.contains(col.name) || o.name == col.name)) match {
        case -1 => die(s"Column '${orderByColumn.map(_.name).orNull}' must exist within the projection")
        case index => index
      }
      results.sortInPlace(results.getField(_, sortColumnID).value, isAscending = orderByColumn.exists(_.isAscending))
    }
    results
  }

}

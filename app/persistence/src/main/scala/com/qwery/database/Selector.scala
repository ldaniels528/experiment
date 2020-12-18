package com.qwery.database

import com.qwery.database.SQLCompiler.implicits.ExpressionFacade
import com.qwery.database.device.BlockDevice
import com.qwery.database.functions._
import com.qwery.database.types.QxAny
import com.qwery.models.expressions.{AllFields, BasicField, Expression, FunctionCall, Field => SQLField}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.language.postfixOps

/**
 * Query Selection
 * @param tableFile the host [[TableFile]]
 */
class Selector(tableFile: TableFile) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tempName = () => java.lang.Long.toString(System.currentTimeMillis(), 36)

  /**
   * Executes a query
   * @param projection the [[Expression field projection]]
   * @param where      the condition which determines which records are included
   * @param groupBy    the optional aggregation columns
   * @param limit      the optional limit
   * @return a [[BlockDevice]] containing the rows
   */
  def select(projection: Seq[Expression],
             where: KeyValues,
             groupBy: Seq[SQLField] = Nil,
             limit: Option[Int] = None): BlockDevice = {
    if (groupBy.nonEmpty) aggregateRows(projection, where, groupBy, limit)
    else selectRows(projection, where, groupBy, limit)
  }

  /**
   * Executes an aggregate query
   * @param projection the [[Expression field projection]]
   * @param where      the [[KeyValues inclusion criteria]]
   * @param groupBy    the columns to group by
   * @param limit      the maximum number of rows for which to return
   * @return the [[BlockDevice results]]
   */
  private def aggregateRows(projection: Seq[Expression], where: KeyValues, groupBy: Seq[SQLField], limit: Option[Int]): BlockDevice = {
    // determine the projection columns and group by columns
    val projectionColumns: Seq[Column] = getProjectedColumns(projection)
    val groupByColumns: Seq[Column] = getColumnsByName(groupBy.map(_.name))
    val groupByColumnNames: Seq[String] = groupByColumns.map(_.name)

    // partition the rows into temporary tables per grouped key
    val tempTables = mutable.Map[String, BlockDevice]()
    val counter = new AtomicInteger(0)
    _while(where) { srcRow =>
      val key = groupByColumnNames.flatMap(srcRow.get).mkString("|")
      implicit val groupDevice: BlockDevice = tempTables.getOrElseUpdate(key, createTempTable(tableFile.device))
      groupDevice.writeRow(srcRow.toBinaryRow)
      limit.isEmpty || limit.exists(counter.addAndGet(1) < _)
    }

    // aggregate the results
    val results = createTempTable(projectionColumns)
    tempTables.values foreach { implicit groupDevice =>
      // compile the projection into aggregators
      val aggExpressions: Seq[AggregateExpr] = getProjectedAggregateExpressions(projection)
      // update the aggregate expressions
      groupDevice.use(_.foreachKVP(keyValues => aggExpressions.foreach(_.update(keyValues))))
      // create the aggregate key-values
      val keyValues = KeyValues(aggExpressions.map(expr => expr.name -> expr.execute): _*)
      // write the aggregated key-values as a row
      results.writeRow(keyValues.toBinaryRow(rowID = results.length)(results))
    }
    results
  }

  /**
   * Executes a query
   * @param projection the [[Expression field projection]]
   * @param where      the condition which determines which records are included
   * @param groupBy    the optional aggregation columns
   * @param limit      the optional limit
   * @return a [[BlockDevice]] containing the rows
   */
  private def selectRows(projection: Seq[Expression],
                         where: KeyValues,
                         groupBy: Seq[SQLField],
                         limit: Option[Int]): BlockDevice = {
    // determine the projection columns
    val projectionColumns = getProjectedColumns(projection)

    // build the result set
    implicit val results: BlockDevice = createTempTable(projectionColumns)
    val counter = new AtomicInteger(0)
    _whileRow(where) { srcRow =>
      val dstRow = getProjectedRow(projection, srcRow)
      results.writeRow(dstRow.toBinaryRow)
      limit.isEmpty || limit.exists(counter.addAndGet(1) < _)
    }
    results
  }

  private def getColumnsByName(names: Seq[String]): Seq[Column] = {
    val projectionColumnsMap = Map(tableFile.device.columns.map(c => c.name -> c): _*)
    (for {
      name <- names
      column <- projectionColumnsMap.get(name)
    } yield column).distinct
  }

  private def getProjectedAggregateExpressions(projection: Seq[Expression]): Seq[AggregateExpr] = {
    projection map {
      case AllFields => die("Aggregation function or constant value expected")
      case f: BasicField => AggregateField(name = f.name)
      case fc@FunctionCall(functionName, args) =>
        val fx = aggregateFunctions.getOrElse(functionName, die(s"Function '$functionName' does not exist"))
        fx(fc.alias || tempName(), args)
      case expression => die(s"Unconverted expression: $expression")
    }
  }

  private def getProjectedColumns(projection: Seq[Expression]): Seq[Column] = {
    projection flatMap {
      case AllFields => tableFile.device.columns
      case f: BasicField => tableFile.device.columns.find(_.name == f.name).toSeq
      case fc@FunctionCall(functionName, args) =>
        val fxTemplate = simpleFunctions.getOrElse(functionName, die(s"Function '$functionName' does not exist"))
        val fx = fxTemplate(fc.alias || tempName(), args)
        Seq(Column(name = fx.name, metadata = ColumnMetadata(`type` = fx.returnType)))
      case expression => Seq(expression.toColumn)
    } distinct
  }

  private def getProjectedRow(projection: Seq[Expression], srcRow: Row): Row = {
    Row(id = srcRow.id, metadata = srcRow.metadata, fields = projection flatMap {
      case AllFields => srcRow.fields
      case f: BasicField => srcRow.fields.find(_.name == f.name).toSeq
      case fc@FunctionCall(functionName, args) =>
        val fxTemplate = simpleFunctions.getOrElse(functionName, die(s"Function '$functionName' does not exist"))
        val fx = fxTemplate(fc.alias || tempName(), args)
        Seq(Field(name = fx.name, metadata = FieldMetadata(), QxAny(Option(fx.execute))))
      case expression =>
        logger.error(s"Unconverted expression: $expression")
        Nil
    })
  }

  @inline
  private def _while(condition: KeyValues)(f: KeyValues => Boolean): Int = {
    var (matches: Int, rowID: ROWID) = (0, 0)
    val eof = tableFile.device.length
    var proceed = true
    while (rowID < eof && proceed) {
      tableFile.getKeyValues(rowID) foreach { row =>
        if (condition.isEmpty || tableFile.isSatisfied(row, condition)) {
          proceed = f(row)
          if (proceed) matches += 1
        }
      }
      rowID += 1
    }
    matches
  }

  @inline
  private def _whileRow(condition: KeyValues)(f: Row => Boolean): Int = {
    var (matches: Int, rowID: ROWID) = (0, 0)
    val eof = tableFile.device.length
    var proceed = true
    while (rowID < eof && proceed) {
      tableFile.getRow(rowID) foreach { row =>
        if (condition.isEmpty || tableFile.isSatisfied(row.toKeyValues, condition)) {
          proceed = f(row)
          if (proceed) matches += 1
        }
      }
      rowID += 1
    }
    matches
  }

}

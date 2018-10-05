package com.qwery.platform.flink

import com.qwery.models.expressions._
import com.qwery.models.{Join, OrderColumn}
import com.qwery.util.OptionHelper._

/**
  * Select Operation for Flink
  * @author lawrence.daniels@gmail.com
  */
case class FlinkSelect(fields: Seq[Expression],
                       from: Option[FlinkInvokable],
                       joins: Seq[Join],
                       groupBy: Seq[Field],
                       orderBy: Seq[OrderColumn],
                       where: Option[Condition],
                       limit: Option[Int],
                       alias: Option[String])
  extends FlinkInvokable {

  override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
    import FlinkQweryCompiler.Implicits._
    import com.qwery.util.OptionHelper.Implicits.Risky._

    // get the data frame to use for selection
    val df = from.flatMap(_.execute(input)) orFail "No input data"

    // build the field selection
    val selection = fields map {
      case field@BasicField(name) => field.alias.map(a => s"$name AS $a") getOrElse name
      case ref@ConstantField(value) => s"${value.compile} AS ${ref.name}"
      case expression: Expression => expression.compile
    } mkString ","

    // perform the query
    val df1 = df.table.select(selection)
    val df2 = where.map(cond => df1.where(cond.compile)) getOrElse df1
    val df3 = limit.map(n => df2.fetch(n)) getOrElse df2
    df3.print()
    df3
  }

}

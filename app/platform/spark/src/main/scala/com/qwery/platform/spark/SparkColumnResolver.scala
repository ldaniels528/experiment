package com.qwery.platform.spark

import com.qwery.models.{Column, Location, TableLike}

/**
  * Spark Column Resolver
  * @author lawrence.daniels@gmail.com
  */
sealed trait SparkColumnResolver {
  def resolve(implicit rc: SparkQweryContext): Seq[Column]
}

case class SparkLocationColumnResolver(location: Location) extends SparkColumnResolver {
  override def resolve(implicit rc: SparkQweryContext): Seq[Column] = {
    import SparkQweryCompiler.Implicits._
    location.resolveColumns
  }
}

case class SparkTableColumnResolver(tableLike: TableLike) extends SparkColumnResolver {
  override def resolve(implicit rc: SparkQweryContext): Seq[Column] = {
    import SparkQweryCompiler.Implicits._
    tableLike.resolveColumns
  }
}


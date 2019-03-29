package com.qwery.platform.sparksql.embedded

import com.qwery.models.{Column, Location, TableLike}

/**
  * Spark Column Resolver
  * @author lawrence.daniels@gmail.com
  */
sealed trait SparkColumnResolver {
  def resolve(implicit rc: EmbeddedSparkContext): Seq[Column]
}

case class SparkLocationColumnResolver(location: Location) extends SparkColumnResolver {
  override def resolve(implicit rc: EmbeddedSparkContext): Seq[Column] = {
    import SparkEmbeddedCompiler.Implicits._
    location.resolveColumns
  }
}

case class SparkTableColumnResolver(tableLike: TableLike) extends SparkColumnResolver {
  override def resolve(implicit rc: EmbeddedSparkContext): Seq[Column] = {
    import SparkEmbeddedCompiler.Implicits._
    tableLike.resolveColumns
  }
}


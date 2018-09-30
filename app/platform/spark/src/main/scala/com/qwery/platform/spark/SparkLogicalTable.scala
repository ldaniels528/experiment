package com.qwery.platform.spark

import com.qwery.models.{Column, TableLike}

/**
  * Spark Logical Table
  * @param name    the name of the table
  * @param columns the table columns
  * @param source  the [[SparkInvokable]]
  */
case class SparkLogicalTable(name: String, columns: Seq[Column], source: SparkInvokable) extends TableLike
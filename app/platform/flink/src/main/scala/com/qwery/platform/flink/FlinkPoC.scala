package com.qwery.platform.flink

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * Qwery Flink proof-of-concept #1
  * @author lawrence.daniels@gmail.com
  */
object FlinkPoC extends App {
  // setup the streaming environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // create the table environment
  val tableEnv = TableEnvironment.getTableEnvironment(env)
  //tableEnv.fromDataStream()
  //tableEnv.toAppendStream()

  def poc1(): Unit = {
    // setup the input table
    val inputSource = CsvTableSource.builder
      .path("./samples/companylist/csv/")
      .ignoreFirstLine
      .fieldDelimiter(",")
      .quoteCharacter('"')
      .field("Symbol", Types.STRING)
      .field("Name", Types.STRING)
      .field("LastSale", Types.STRING)
      .field("MarketCap", Types.STRING)
      .field("IPOyear", Types.STRING)
      .field("Sector", Types.STRING)
      .field("Industry", Types.STRING)
      .field("SummaryQuote", Types.STRING)
      .field("Reserved", Types.STRING)
      .build

    // register the table
    tableEnv.registerTableSource("companylist", inputSource)

    // define the table
    val table = tableEnv
      .scan("companylist")
      .where("Sector = 'Finance'")
      .select("Symbol,Sector,Industry,LastSale")

    // perform the transformation
    //copyItAsCSV(table)
    printIt(table)

    // start the process
    env.execute(jobName = "FlinkPoC1")
  }

  def poc2(): Unit = {

  }

  def copyItAsCSV(table: Table): Unit = {
    val sink = new CsvTableSink(path = "./temp/companylist/flink/csv", fieldDelim = Some(","), numFiles = None, writeMode = Some(WriteMode.OVERWRITE))
    table.writeToSink(sink)
  }

  def printIt(table: Table): Unit = {
    val stream = tableEnv.toAppendStream[Row](table)
    stream.print
  }

}

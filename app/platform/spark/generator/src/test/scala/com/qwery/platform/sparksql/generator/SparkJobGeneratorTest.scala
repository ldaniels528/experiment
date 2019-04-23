package com.qwery.platform.sparksql.generator

import com.qwery.language.SQLLanguageParser
import org.scalatest.FunSpec

/**
  * Spark Job Generator Test Suite
  * @author lawrence.daniels@gmail.com
  */
class SparkJobGeneratorTest extends FunSpec {

  describe(classOf[SparkJobGenerator].getSimpleName) {

    it("should generate a class from an SQL statement") {
      SparkJobGenerator.main(Array(
        "--app-name", "Customer Demo",
        "--class-name", "com.github.ldaniels528.qwery.CustomerDemo",
        "--input-path", "./app/platform/spark/generator/src/test/resources/join_using.sql",
        "--output-path", "../temp/projects/customer_demo"
      ))
    }

    it("should compile and execute: companylist.sql") {
      SparkJobGenerator.main(Array(
        "--app-name", "Company List Demo",
        "--input-path", "./samples/sql/companylist/companylist.sql",
        "--output-path", "../temp/projects/company_list_demo",
        "--class-name", "com.github.ldaniels528.qwery.Companylist"
      ))
    }

    it("should compile and execute: files.sql") {
      SparkJobGenerator.main(Array(
        "--app-name", "Files Demo",
        "--input-path", "./samples/sql/misc/files.sql",
        "--output-path", "../temp/projects/files_demo",
        "--class-name", "com.github.ldaniels528.qwery.Files"
      ))
    }


    it("should compile and execute: join.sql") {
      SparkJobGenerator.main(Array(
        "--app-name", "Joins Demo",
        "--input-path", "./samples/sql/misc/joins.sql",
        "--output-path", "../temp/projects/joins_demo",
        "--class-name", "com.github.ldaniels528.qwery.Joins"
      ))
    }

    it("should compile and execute: procedure.sql") {
      SparkJobGenerator.main(Array(
        "--app-name", "Procedure Demo",
        "--input-path", "./samples/sql/misc/procedure.sql",
        "--output-path", "../temp/projects/procedure_demo",
        "--class-name", "com.github.ldaniels528.qwery.Procedures"
      ))
    }

    it("should compile and execute: views.sql") {
      SparkJobGenerator.main(Array(
        "--app-name", "View Demo",
        "--input-path", "./samples/sql/misc/views.sql",
        "--output-path", "../temp/projects/view_demo",
        "--class-name", "com.github.ldaniels528.qwery.Views"
      ))
    }

    it("should generate the OilGasSecurities Spark job main class-only") {
      implicit val settings: ApplicationSettings = ApplicationSettings.fromArgs(args =
        "--app-name", "Securities Demo",
        "--input-path", "./scripts/daily-report.sql",
        "--output-path", "../temp/projects/securities_demo",
        "--class-name", "com.github.ldaniels528.qwery.OilGasSecurities"
      )

      val model = SQLLanguageParser.parse(
        """|-- define the input source
           |CREATE EXTERNAL TABLE Securities (
           |        Symbol STRING,
           |        Name STRING,
           |        LastSale STRING,
           |        MarketCap STRING,
           |        IPOyear STRING,
           |        Sector STRING,
           |        Industry STRING,
           |        SummaryQuote STRING,
           |        Reserved STRING
           |    )
           |    ROW FORMAT DELIMITED
           |    FIELDS TERMINATED BY ','
           |    STORED AS INPUTFORMAT 'CSV'
           |    WITH HEADERS ON
           |    WITH NULL VALUES AS 'n/a'
           |    LOCATION './samples/companylist/csv/';
           |
           |-- define the output source
           |CREATE EXTERNAL TABLE OilGasSecurities (
           |        Symbol STRING,
           |        Name STRING,
           |        LastSale DOUBLE,
           |        MarketCap STRING,
           |        IPOyear STRING,
           |        Sector STRING,
           |        Industry STRING,
           |        SummaryQuote STRING,
           |        Reserved STRING
           |    )
           |    ROW FORMAT DELIMITED
           |    FIELDS TERMINATED BY ','
           |    STORED AS OUTPUTFORMAT 'CSV'
           |    LOCATION './temp/flink/companylist/csv/';
           |
           |-- process the data
           |INSERT INTO TABLE OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |SELECT Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |FROM Securities
           |WHERE Industry = 'Oil/Gas Transmission'
           |""".stripMargin
      )
      implicit val ctx: CompileContext = CompileContext(model)
      val generator = new SparkJobGenerator()
      generator.generate(model)
    }

  }

}

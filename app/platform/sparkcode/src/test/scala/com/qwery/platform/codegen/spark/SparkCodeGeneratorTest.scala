package com.qwery.platform.codegen.spark

import com.qwery.language.SQLLanguageParser
import org.scalatest.FunSpec

/**
  * Spark Code Generator Test Suite
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeGeneratorTest extends FunSpec {

  describe(classOf[SparkCodeGenerator].getSimpleName) {

    it("should generate a Spark job") {
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
      val generator = new SparkCodeGenerator(
        className = "OilGasSecurities",
        packageName = "com.qwery.platform.codegen.spark",
        outputPath = "./app/platform/sparkcode/src/test/scala")
      generator.generate(model)
    }

  }

}

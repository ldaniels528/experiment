package com.qwery.platform.codegen.spark

import com.qwery.language.SQLLanguageParser
import com.qwery.platform.spark.QwerySparkJob
import org.scalatest.FunSpec

/**
  * AdBook Ingestion Test
  * @author lawrence.daniels@gmail.com
  */
class AdBookIngestTest extends FunSpec {

  describe(classOf[SQLLanguageParser].getSimpleName) {

    it("should compile and execute: kbb_ab_client.sql") {
      QwerySparkJob.main(Array("./samples/sql/adbook/kbb_ab_client.sql"))
    }

    it("should compile and execute: kbb_lkp_dfp_o1_advertiser.sql") {
      QwerySparkJob.main(Array("./samples/sql/adbook/kbb_lkp_dfp_o1_advertiser.sql"))
    }

    it("should compile and execute: adbook-client.sql") {
      QwerySparkJob.main(Array("./samples/sql/adbook/adbook-client.sql"))
    }

  }

}

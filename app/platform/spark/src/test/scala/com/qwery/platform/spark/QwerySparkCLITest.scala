package com.qwery.platform.spark

import org.scalatest.FunSpec

/**
  * Qwery Spark CLI Tests
  * @author lawrence.daniels@gmail.com
  */
class QwerySparkCLITest extends FunSpec {

  describe(QwerySparkCLI.getClass.getSimpleName) {

    it("should compile and execute: companylist.sql") {
      QwerySparkCLI.main(Array("./samples/sql/companylist.sql"))
    }

    it("should compile and execute: joins.sql") {
      QwerySparkCLI.main(Array("./samples/sql/joins.sql"))
    }

    it("should compile and execute: procedure.sql") {
      QwerySparkCLI.main(Array("./samples/sql/procedure.sql"))
    }

    it("should compile and execute: liveramp.sql") {
      QwerySparkCLI.main(Array("./samples/sql/liveramp.sql"))
    }

  }

}

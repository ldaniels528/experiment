package com.qwery.platform.spark

import com.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Qwery Spark Job Tests
  * @author lawrence.daniels@gmail.com
  */
class QwerySparkJobTest extends FunSpec {

  describe(QwerySparkJob.getObjectSimpleName) {

    it("should compile and execute: companylist.sql") {
      QwerySparkJob.main(Array("./samples/sql/companylist/companylist.sql"))
    }

    it("should compile and execute: files.sql") {
      QwerySparkJob.main(Array("./samples/sql/misc/files.sql"))
    }


    it("should compile and execute: joins.sql") {
      QwerySparkJob.main(Array("./samples/sql/misc/joins.sql"))
    }

    it("should compile and execute: procedure.sql") {
      QwerySparkJob.main(Array("./samples/sql/misc/procedure.sql"))
    }

    it("should compile and execute: liveramp.sql") {
      QwerySparkJob.main(Array("./samples/sql/misc/liveramp.sql"))
    }

    it("should compile and execute: boot.sql") {
      QwerySparkJob.main(Array())
    }

  }

}

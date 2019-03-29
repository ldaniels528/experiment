package com.qwery.platform.sparksql.embedded

import com.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Qwery Spark Job Tests
  * @author lawrence.daniels@gmail.com
  */
class EmbeddedSparkJobTest extends FunSpec {

  describe(EmbeddedSparkJob.getObjectSimpleName) {

    it("should compile and execute: companylist.sql") {
      EmbeddedSparkJob.main(Array("./samples/sql/companylist/companylist.sql"))
    }

    it("should compile and execute: files.sql") {
      EmbeddedSparkJob.main(Array("./samples/sql/misc/files.sql"))
    }


    it("should compile and execute: joins.sql") {
      EmbeddedSparkJob.main(Array("./samples/sql/misc/joins.sql"))
    }

    it("should compile and execute: procedure.sql") {
      EmbeddedSparkJob.main(Array("./samples/sql/misc/procedure.sql"))
    }

    it("should compile and execute: liveramp.sql") {
      EmbeddedSparkJob.main(Array("./samples/sql/misc/liveramp.sql"))
    }

    it("should compile and execute: boot.sql") {
      EmbeddedSparkJob.main(Array())
    }

  }

}

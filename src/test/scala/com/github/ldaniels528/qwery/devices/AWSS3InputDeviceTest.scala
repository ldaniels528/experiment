package com.github.ldaniels528.qwery.devices

import java.io.{File, FileReader}
import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.ops.RootScope
import com.github.ldaniels528.qwery.util.ResourceHelper._
import org.scalatest.FunSpec

/**
  * AWS S3 Input Device Test
  * @author lawrence.daniels@gmail.com
  */
class AWSS3InputDeviceTest extends FunSpec {
  private val file = new File("./s3.properties")
  private val scope = RootScope()

  describe("AWSS3InputDevice") {

    it("it should connect to S3 and list the files") {
      if (file.exists()) {
        val config = new JProperties()
        config.load(new FileReader(file))
        info(s"config: $config")

        val source = AWSS3InputDevice(bucketName = "ldaniels3", keyName = "companylist.csv", config = config)
        source.open(scope)
        source use { device =>
          var record: Option[Record] = None
          do {
            record = device.read()
            info(s"${record.map(r => new String(r.data)).orNull}")
          } while (record.nonEmpty)
        }
      } else {
        alert(s"You must set ${file.getName} to run this test...")
      }
    }
  }

}

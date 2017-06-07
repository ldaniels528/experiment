package com.github.ldaniels528.qwery.sources

import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.devices.{AWSS3InputDevice, KafkaInputDevice}
import com.github.ldaniels528.qwery.ops.Hints
import org.scalatest.FunSpec

/**
  * Source URL Parser Test
  * @author lawrence.daniels@gmail.com
  */
class SourceUrlParserTest extends FunSpec {

  describe("SourceUrlParser") {
    val parser = new SourceUrlParser {}

    it("should return the specified Kafka / Arvo source") {
      val source = parser.parseInputSource("kafka:avro://server?topic=X&group_id=Y&schema=./pixall-v5.avsc.json", hints = Some(
        Hints(properties = Some(new JProperties()))
      ))
      assert(source.exists(_.getClass == classOf[AvroInputSource]))
      assert(source.exists(_.device.getClass == classOf[KafkaInputDevice]))
    }

    it("should return the specified S3 / CSV source") {
      val source = parser.parseInputSource("s3:csv://ldaniels3/companylist.csv?region=us-west-1", hints = Some(
        Hints(properties = Some(new JProperties()))
      ))
      assert(source.exists(_.getClass == classOf[DelimitedInputSource]))
      assert(source.exists(_.device.getClass == classOf[AWSS3InputDevice]))
    }
  }

}

package com.github.ldaniels528.qwery

import java.io.FileInputStream
import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.ops.{Hints, RootScope}
import com.github.ldaniels528.qwery.sources.InputSource
import com.github.ldaniels528.qwery.util.OptionHelper.Risky._

/**
  * Kafka Test
  * @author lawrence.daniels@gmail.com
  */
object KafkaTest {
  private val scope = RootScope()

  def main(args: Array[String]): Unit = {
    val hints = Hints(properties = {
      val props = new JProperties()
      props.load(new FileInputStream("./pixall-config.properties"))
      props
    })

    InputSource("kafka:avro://kafka.non-prod.epd-analytics.dealer.com:9093?topic=pixall_parsed_v5&group_id=ldtest&schema=./pixall-v5.avsc.json", hints)
      .foreach { source =>
        source.open(scope)
        for(_ <- 1 to 5 ) source.read().foreach(r => println(s"$r"))
        source.close()
      }
  }

}

package com.github.ldaniels528.qwery.devices

import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.sources.JDBCOutputSource

import scala.collection.concurrent.TrieMap

/**
  * Output Device Factory
  * @author lawrence.daniels@gmail.com
  */
trait OutputDeviceFactory {

  /**
    * Returns a compatible output device for the given URL.
    * @param url the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[OutputDevice output device]]
    */
  def parseOutputURL(url: String, hints: Option[Hints]): Option[OutputDevice]

}

/**
  * Output Device Factory Singleton
  * @author lawrence.daniels@gmail.com
  */
object OutputDeviceFactory extends OutputDeviceFactory {
  private[this] val factories = TrieMap[String, OutputDeviceFactory]()

  // add the built-in device types
  add("file", TextFileOutputDevice)
  add("jdbc", JDBCOutputSource)
  add("kafka", KafkaOutputDevice)
  add("s3", AWSS3OutputDevice)

  /**
    * Adds a new device to the factory
    * @param prefix  the given device prefix (e.g. "jdbc")
    * @param factory the given [[OutputDeviceFactory output device factory]]
    */
  def add(prefix: String, factory: OutputDeviceFactory): Unit = factories(prefix.toLowerCase()) = factory

  /**
    * Returns a compatible output device for the given URL.
    * @param url the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[OutputDevice output device]]
    */
  def parseOutputURL(url: String, hints: Option[Hints]): Option[OutputDevice] = {
    (for {
      prefix <- url.split("[:]", 2).headOption.map(_.toLowerCase())
      factory <- factories.get(prefix)
      device <- factory.parseOutputURL(url, hints)
    } yield device) ?? TextFileOutputDevice.parseOutputURL(url, hints)
  }

}
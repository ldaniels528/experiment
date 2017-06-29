package com.github.ldaniels528.qwery.devices

import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.sources.JDBCInputSource
import com.github.ldaniels528.qwery.util.OptionHelper._

import scala.collection.concurrent.TrieMap

/**
  * Input Device Factory
  * @author lawrence.daniels@gmail.com
  */
trait InputDeviceFactory {

  /**
    * Returns a compatible input device for the given URL.
    * @param url the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[InputDevice input device]]
    */
  def parseInputURL(url: String, hints: Option[Hints]): Option[InputDevice]

}

/**
  * Device Factory Singleton
  * @author lawrence.daniels@gmail.com
  */
object InputDeviceFactory extends InputDeviceFactory {
  private[this] val factories = TrieMap[String, InputDeviceFactory]()

  // add the built-in device types
  add("file", TextFileInputDevice)
  add("http", TextFileInputDevice)
  add("jdbc", JDBCInputSource)
  add("kafka", KafkaInputDevice)
  add("s3", AWSS3InputDevice)

  /**
    * Adds a new device type to the factory
    * @param prefix  the given device prefix (e.g. "jdbc")
    * @param factory the given [[InputDeviceFactory input device factory]]
    */
  def add(prefix: String, factory: InputDeviceFactory): Unit = factories(prefix.toLowerCase()) = factory

  /**
    * Returns a compatible input device for the given URL.
    * @param url the given URL (e.g. "jdbc:mysql://localhost/test")
    * @return an option of the [[InputDevice input device]]
    */
  def parseInputURL(url: String, hints: Option[Hints]): Option[InputDevice] = {
    (for {
      prefix <- url.split("[:]", 2).headOption.map(_.toLowerCase())
      factory <- factories.get(prefix)
      device <- factory.parseInputURL(url, hints)
    } yield device) ?? TextFileInputDevice.parseInputURL(url, hints)
  }

}
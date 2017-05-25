package com.github.ldaniels528.qwery.devices

import akka.actor.ActorRef

/**
  * Asynchronous Input Device
  * @author lawrence.daniels@gmail.com
  */
trait AsyncInputDevice extends Device {

  def read(actor: ActorRef): Unit

}

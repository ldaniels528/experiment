package com.github.ldaniels528.qwery.etl.events

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import com.github.ldaniels528.qwery.actors.QweryActorSystem
import com.github.ldaniels528.qwery.etl.{ETLConfig, ScriptSupport}
import com.github.ldaniels528.qwery.ops.{Executable, Scope}
import com.github.ldaniels528.qwery.util.JSONSupport
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * Represents a Scheduled Event
  * @param uid        the schedule event unique identifier
  * @param name       the name of the scheduled event
  * @param times      indicates when the event should be fired
  * @param executable the given [[Executable code]] to execute when the event is fired
  */
case class ScheduledEvent(uid: UUID, name: String, times: Array[String], executable: Executable) {
  private lazy val log = LoggerFactory.getLogger(getClass)
  var scheduledTimes: Seq[Date] = Nil

  def update(config: ETLConfig)(implicit ec: ExecutionContext): Unit = {
    val sdf = new SimpleDateFormat("MM/dd/yyyy 'at' hh:mm:ss a")
    val now = new Date()
    if (scheduledTimes.isEmpty) {
      scheduledTimes = getNextRunTimes
      scheduledTimes foreach { scheduledTime =>
        log.info(s"Scheduling '$name' to execute on ${sdf.format(scheduledTime)}")
        val delay = (scheduledTime.getTime - now.getTime).millis
        QweryActorSystem.scheduler.scheduleOnce(delay) {
          log.info(s"[$uid] Executing scheduled event '$name'...")
          Try(executable.execute(Scope.root())) match {
            case Success(_) => log.info(s"[$uid] Completed Successfully")
            case Failure(e) => log.error(s"[$uid] Failed: ${e.getMessage}")
          }
        }
      }
    }

    scheduledTimes = getNextRunTimes
  }

  def getNextRunTimes: Seq[Date] = {
    times.map { s =>
      val (hour, minute, second) = s.split("[:]").toList match {
        case hh :: mm :: Nil => (hh.toInt, mm.toInt, 0)
        case hh :: mm :: ss :: Nil => (hh.toInt, mm.toInt, ss.toInt)
        case _ =>
          throw new IllegalArgumentException(s"Invalid time value '$s'. Expected hh:mm[:ss] format.")
      }
      getNextRunTime(hour, minute, second)
    }
  }

  private def getNextRunTime(hour: Int, minute: Int, second: Int): Date = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, hour)
    cal.set(Calendar.MINUTE, minute)
    cal.set(Calendar.SECOND, second)

    val now = new Date()
    if (cal.getTime.after(now)) cal.getTime
    else {
      val dayOfYear = cal.get(Calendar.DAY_OF_YEAR) + 1
      cal.set(Calendar.DAY_OF_YEAR, dayOfYear)
      cal.getTime
    }
  }

}

/**
  * Scheduled Event Companion
  * @author lawrence.daniels@gmail.com
  */
object ScheduledEvent extends JSONSupport {
  private[this] lazy val log = LoggerFactory.getLogger(getClass)

  /**
    * Loads the scheduled events found in ./config/scheduled-events.json
    */
  def loadScheduledEvents(config: ETLConfig, configDir: File): List[ScheduledEvent] = {
    val scheduledEventsFile = new File(configDir, "scheduled-events.json")
    if (scheduledEventsFile.exists()) {
      log.info(s"Loading scheduled events from '${scheduledEventsFile.getCanonicalPath}'...")
      val scheduledEventsJs = parseJsonAs[List[ScheduledEventRaw]](Source.fromFile(scheduledEventsFile).mkString)
      scheduledEventsJs.map(_.toModel(config))
    }
    else Nil
  }

  /**
    * Represents a scheduled event JSON object
    * @param name   the name of the scheduled event
    * @param script the given script to execute when the event is fired
    * @param times  indicates when the event should be fired
    */
  case class ScheduledEventRaw(name: String, script: String, times: Array[String]) extends ScriptSupport {

    def toModel(config: ETLConfig): ScheduledEvent = {
      ScheduledEvent(UUID.randomUUID(), name, times, compileScript(config, name, script))
    }

  }

}
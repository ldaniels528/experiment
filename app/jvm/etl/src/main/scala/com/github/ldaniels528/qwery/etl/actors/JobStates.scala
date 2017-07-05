package com.github.ldaniels528.qwery.etl.actors

/**
  * Job States Enumeration
  * @author lawrence.daniels@gmail.com
  */
object JobStates {
  type JobState = String

  val NEW: JobState = "NEW"
  val CLAIMED: JobState = "CLAIMED"
  val FAILED: JobState = "FAILED"
  val QUEUED: JobState = "QUEUED"
  val RUNNING: JobState = "RUNNING"
  val PAUSED: JobState = "PAUSED"
  val STOPPED: JobState = "STOPPED"
  val SUCCESS: JobState = "SUCCESS"

  def values: Iterator[JobState] = Seq(NEW, QUEUED, RUNNING, STOPPED, SUCCESS).iterator

}

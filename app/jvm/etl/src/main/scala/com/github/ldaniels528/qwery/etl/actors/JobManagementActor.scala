package com.github.ldaniels528.qwery.etl.actors

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.etl.ETLConfig
import com.github.ldaniels528.qwery.etl.actors.JobManagementActor._
import com.github.ldaniels528.qwery.etl.actors.JobStates.JobState
import com.github.ldaniels528.qwery.etl.rest.JobClient

/**
  * Job Management Actor
  * @param config the given [[ETLConfig ETL configuration]]
  */
class JobManagementActor(config: ETLConfig) extends Actor with ActorLogging with JobClient {
  private implicit val dispatcher = context.dispatcher

  override def baseUrl = s"http://${config.supervisor}"

  override def receive: Receive = {
    case ChangeJobState(job, state) => reflect(changeState(job, state))
    case CheckForJobs(slaveID) => reflect(checkoutJob(slaveID))
    case CreateJob(job) => reflect(createJob(job))
    case UpdateStatistics(job, stats) => reflect(updateStatistics(job, stats))
    case message =>
      log.warning(s"Unexpected message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

}
                                                    
/**
  * Job Management Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object JobManagementActor {

  case class CheckForJobs(slaveID: String)

  case class CreateJob(job: Job)

  case class ChangeJobState(job: Job, state: JobState)

  case class UpdateStatistics(job: Job, stats: List[JobStatistics])

}
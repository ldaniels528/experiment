package com.github.ldaniels528.qwery.devices

import java.io._

import com.amazonaws.services.s3.model.PutObjectRequest
import com.github.ldaniels528.qwery.devices.AWSS3InputDevice.{getProfileCredentials, getS3Client, getStaticCredentials}
import com.github.ldaniels528.qwery.devices.TextFileOutputDevice.getBufferedWriter
import com.github.ldaniels528.qwery.ops.{Hints, Scope}

import scala.util.Try

/**
  * AWS S3 File Output Device
  * @author lawrence.daniels@gmail.com
  */
case class AWSS3OutputDevice(bucketName: String,
                             keyName: String,
                             region: Option[String],
                             profile: Option[String],
                             hints: Option[Hints])
  extends OutputDevice {
  private var writer: Option[BufferedWriter] = None
  private var file: Option[File] = None
  private var offset: Long = _

  override def close(): Unit = writer.foreach { out =>
    Try(out.close())
    file foreach upload
  }

  override def open(scope: Scope): Unit = {
    super.open(scope)
    file = Option(File.createTempFile("aws", "s3"))
    writer = file.map(f => getBufferedWriter(f.getAbsolutePath, hints))
    offset = 0L
  }

  def upload(uploadFile: File): Unit = {
    val config = hints.flatMap(_.properties)
    val credentialsProvider = config.flatMap(getStaticCredentials).getOrElse(getProfileCredentials(profile))
    val s3client = getS3Client(credentialsProvider, region)
    s3client.putObject(new PutObjectRequest(bucketName, keyName, uploadFile))
  }

  override def write(record: Record): Unit = writer.foreach { out =>
    statsGen.update(records = 1, bytesRead = record.data.length)
    out.write(new String(record.data))
    out.newLine()
  }

}

/**
  * AWS S3 File Output Device
  * @author lawrence.daniels@gmail.com
  */
object AWSS3OutputDevice extends OutputDeviceFactory with SourceUrlParser {

  /**
    * Returns a compatible output device for the given URL.
    * @param url the given URL (e.g. "s3://ldaniels3/companylist.csv?region=us-west-1&profile=ldaniels3")
    * @return an option of the [[OutputDevice output device]]
    */
  override def parseOutputURL(url: String, hints: Option[Hints]): Option[OutputDevice] = {
    val comps = parseURI(url)
    for {
      bucket <- comps.host if url.toLowerCase.startsWith("s3:")
      key <- comps.path
      region = comps.params.get("region")
      profile = comps.params.get("profile")
    } yield AWSS3OutputDevice(bucketName = bucket, keyName = key, region = region, profile = profile, hints = hints)
  }

}
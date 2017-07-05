package com.github.ldaniels528.qwery.etl.actors

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.github.ldaniels528.qwery.etl.ETLConfig
import org.slf4j.LoggerFactory

/**
  * File Moving
  * @author lawrence.daniels@gmail.com
  */
trait FileMoving {
  private[this] val log = LoggerFactory.getLogger(getClass)

  def moveToArchive(file: File, compress: Boolean)(implicit config: ETLConfig): Option[File] = {
    file match {
      case f if f.exists() =>
        val ts = new Date()
        val year = new SimpleDateFormat("yyyy").format(ts)
        val month = new SimpleDateFormat("MM").format(ts)
        val day = new SimpleDateFormat("dd").format(ts)
        val time = new SimpleDateFormat("hhmmss").format(ts)
        val archiveFile = Seq(year, month, day, time, file.getName)
          .foldLeft(config.archiveDir) { (directory, name) => new File(directory, name) }
        log.info(s"Moving '${file.getName}' to '${archiveFile.getAbsolutePath}'")
        val archiveDirectory = archiveFile.getParentFile
        if (!archiveDirectory.exists()) archiveDirectory.mkdirs()
        if (file.renameTo(archiveFile)) Some(archiveFile) else None

      case f =>
        log.warn(s"File '${f.getCanonicalPath}' does not exist")
        None
    }
  }

  /**
    * Moves the file to the failed directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  def moveToFailed(pid: String, file: File)(implicit config: ETLConfig): Option[File] = {
    moveToDirectory(pid, file, config.failedDir)
  }

  /**
    * Moves the file to the work directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  def moveToWork(pid: String, file: File)(implicit config: ETLConfig): Option[File] = {
    moveToDirectory(pid, file, config.workDir)
  }

  /**
    * Moves the file to the directory
    * @param pid  the given process ID
    * @param file the file to move
    * @return true, if the file was successfully moved
    */
  def moveToDirectory(pid: String, file: File, directory: File)(implicit config: ETLConfig): Option[File] = {
    if (file.exists()) {
      val targetDirectory = new File(directory, pid.toString)
      val targetFile = new File(targetDirectory, file.getName)
      val renamed = targetDirectory.mkdirs() && file.renameTo(targetFile)
      if (renamed) Some(targetFile) else {
        log.warn(s"[$pid] File '${file.getAbsolutePath}' could not be moved")
        None
      }
    } else {
      log.warn(s"[$pid] File '${file.getCanonicalPath}' does not exist")
      None
    }
  }

}

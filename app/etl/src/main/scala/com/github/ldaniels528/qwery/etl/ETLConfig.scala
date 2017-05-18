package com.github.ldaniels528.qwery.etl

import java.io.File

/**
  * ETL Configuration
  * @author lawrence.daniels@gmail.com
  */
class ETLConfig(val baseDir: File) {
  val archiveDir: File = new File(baseDir, "archive")
  val failedDir: File = new File(baseDir, "failed")
  val incomingDir: File = new File(baseDir, "incoming")
  val workDir: File = new File(baseDir, "work")

  // installation checks
  ensureSubdirectories(baseDir)

  private def ensureSubdirectories(baseDir: File) = {
    // make sure it exists
    if (!baseDir.exists || !baseDir.isDirectory)
      throw new IllegalStateException(s"Qwery Home directory '${baseDir.getAbsolutePath}' does not exist")

    val subDirectories = Seq("incoming", "work", "archive").map(new File(baseDir, _))
    subDirectories.foreach { directory =>
      if (!directory.exists()) directory.mkdir()
    }
  }

}

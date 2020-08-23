package com.qwery.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.qwery.util.ResourceHelper._
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

/**
 * Add compression support
 */
trait Compression {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def compressBytes(bytes: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream(bytes.length)
    new GZIPOutputStream(out) use { gzos =>
      gzos.write(bytes)
      gzos.finish()
      gzos.flush()
    }
    val compressedBytes = out.toByteArray
    //logger.info(s"compressed ${bytes.length} to ${compressedBytes.length} bytes")
    compressedBytes
  }

  def decompressBytes(bytes: Array[Byte]): Array[Byte] = {
    val in = new ByteArrayInputStream(bytes)
    val out = new ByteArrayOutputStream(1024 * 1024)
    new GZIPInputStream(in).use(IOUtils.copy(_, out))
    val decompressedBytes = out.toByteArray
    //logger.info(s"decompressed ${bytes.length} to ${decompressedBytes.length} bytes")
    decompressedBytes
  }

}

/**
 * Compression Companion
 */
object Compression extends Compression {

  /**
   * Compression ByteArray Extensions
   * @param bytes the given [[Array byte array]]
   */
  final implicit class CompressionByteArrayExtensions(val bytes: Array[Byte]) extends AnyVal {

    def compress: Array[Byte] = compressBytes(bytes)

    def compressOrNah(implicit fmd: FieldMetaData): Array[Byte] = {
      if (fmd.isCompressed && fmd.isNotNull) compressBytes(bytes) else bytes
    }

    def decompressOrNah(implicit fmd: FieldMetaData): Array[Byte] = {
      if (fmd.isCompressed && fmd.isNotNull) decompressBytes(bytes) else bytes
    }
  }

  /**
   * Compression ByteBuffer Extensions
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CompressionByteBufferExtensions(val buf: ByteBuffer) extends AnyVal {

    def compress: ByteBuffer = wrap(compressBytes(buf.array()))

    def compressOrNah(implicit fmd: FieldMetaData): ByteBuffer = {
      if (fmd.isCompressed && fmd.isNotNull) wrap(compressBytes(buf.array())) else buf
    }

    def decompressOrNah(implicit fmd: FieldMetaData): ByteBuffer = {
      if (fmd.isCompressed && fmd.isNotNull) wrap(decompressBytes(buf.array())) else buf
    }
  }

}

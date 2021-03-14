package com.qwery.database.util

import com.qwery.database.models.FieldMetadata
import com.qwery.util.ResourceHelper.AutoClose
import org.apache.commons.io.IOUtils
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
 * Adds compression/decompression support
 */
trait Compression {

  def compressGZIP(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new GZIPOutputStream(baos) use { gzos =>
        gzos.write(bytes)
        gzos.finish()
        gzos.flush()
      }
      baos.toByteArray
    }
  }

  def decompressGZIP(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayInputStream(bytes) use { bais =>
      new ByteArrayOutputStream(bytes.length) use { baos =>
        new GZIPInputStream(bais).use(IOUtils.copy(_, baos))
        baos.toByteArray
      }
    }
  }

  def compressSnappy(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new SnappyOutputStream(baos).use(_.write(bytes))
      baos.toByteArray
    }
  }

  def decompressSnappy(bytes: Array[Byte]): Array[Byte] = {
    new ByteArrayOutputStream(bytes.length) use { baos =>
      new ByteArrayInputStream(bytes) use { bais =>
        new SnappyInputStream(bais).use(IOUtils.copy(_, baos))
      }
      baos.toByteArray
    }
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

    @inline def compress: Array[Byte] = compressGZIP(bytes)

    @inline
    def compressOrNah(implicit fmd: FieldMetadata): Array[Byte] = {
      if (fmd.isCompressed && fmd.isActive) compressGZIP(bytes) else bytes
    }

    @inline
    def decompressOrNah(implicit fmd: FieldMetadata): Array[Byte] = {
      if (fmd.isCompressed && fmd.isActive) decompressGZIP(bytes) else bytes
    }
  }

  /**
   * Compression ByteBuffer Extensions
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CompressionByteBufferExtensions(val buf: ByteBuffer) extends AnyVal {

    @inline def compress: ByteBuffer = wrap(compressGZIP(buf.array()))

    @inline
    def compressOrNah(implicit fmd: FieldMetadata): ByteBuffer = {
      if (fmd.isCompressed && fmd.isActive) wrap(compressGZIP(buf.array())) else buf
    }

    @inline
    def decompressOrNah(implicit fmd: FieldMetadata): ByteBuffer = {
      if (fmd.isCompressed && fmd.isActive) wrap(decompressGZIP(buf.array())) else buf
    }
  }

}

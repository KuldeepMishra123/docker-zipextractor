package com.kulexample


import java.io.{BufferedInputStream, InputStream}

import com.kulexample.MainRunner.spark
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.CloseShieldInputStream
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

object Unpacker {

  /** Unpack a compressed archive from an input stream; for example, a stream of
   * bytes from a tar.gz or tar.xz archive.
   *
   * The result is an iterator of 2-tuples, one for each entry in the archive:
   *
   *   - An ArchiveEntry instance, with information like name, size and whether
   *     the entry is a file or directory
   *   - An InputStream of all the bytes in this particular entry
   *
   */
  def open(inputStream: InputStream): Try[Iterator[(ArchiveEntry, InputStream)]] =
    for {
      uncompressedInputStream <- createUncompressedStream(inputStream)
      archiveInputStream <- createArchiveStream(uncompressedInputStream)
      iterator = createIterator(archiveInputStream)
    } yield iterator

  def createUncompressedStream(
                                inputStream: InputStream): Try[CompressorInputStream] =
    Try {
      new CompressorStreamFactory().createCompressorInputStream(
        getMarkableStream(inputStream)
      )
    }

  def createArchiveStream(
                           uncompressedInputStream: CompressorInputStream): Try[ArchiveInputStream] =
    Try {
      new ArchiveStreamFactory()
        .createArchiveInputStream(
          getMarkableStream(uncompressedInputStream)
        )
    }

  def createIterator(
                      archiveInputStream: ArchiveInputStream): Iterator[(ArchiveEntry, InputStream)] =
    new Iterator[(ArchiveEntry, InputStream)] {
      var latestEntry: ArchiveEntry = _

      override def hasNext: Boolean = {
        latestEntry = archiveInputStream.getNextEntry
        latestEntry != null
      }

      override def next(): (ArchiveEntry, InputStream) =
        (latestEntry, new CloseShieldInputStream(archiveInputStream))
    }

  def getMarkableStream(inputStream: InputStream): InputStream =
    if (inputStream.markSupported())
      inputStream
    else
      new BufferedInputStream(inputStream)

  def unzipTar(readPath: String, hdfsWritePath: String, fs: FileSystem) = {
    val hdfsReadPath = new Path(readPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputStream = fs.open(hdfsReadPath)

    val zipIterator: Try[Iterator[(ArchiveEntry, InputStream)]] = Unpacker.open(inputStream)
    zipIterator.get.foreach { case (archiveEntry, is) =>
      val fileName = archiveEntry.getName
      val targetPath = hdfsWritePath + fileName
      println("FileName:  " + fileName)
      println("TARGET_PATH:  " + targetPath)
      if (fileName.endsWith(".json")) {
        val hdfswritepath = new Path(targetPath)
        println("After HDFS WritePath:")
        val fos = fs.create(hdfswritepath, true)
        try {
          IOUtils.copyLarge(is, fos)
          println(s"$fileName extraction is done .")
        }
        finally (fos.close())
      }
      else {
        println("Not Json file")
      }
    }
    //    val fileNamesList = (for ((archiveEntry, is) <- zipIterator.get) yield archiveEntry.getName).toList
    //    fileNamesList.filter(file => file.endsWith("json"))
  }
}

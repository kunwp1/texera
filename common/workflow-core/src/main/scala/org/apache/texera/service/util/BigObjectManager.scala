/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.service.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.amber.core.tuple.BigObjectPointer
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.BIG_OBJECT

import java.io.{Closeable, InputStream}
import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * Stream for reading big objects from S3.
  * All read methods guarantee to read the full requested amount (or until EOF).
  */
class BigObjectStream(private val inputStream: InputStream) extends Closeable {

  @volatile private var closed = false

  private def ensureOpen(): Unit =
    if (closed) throw new IllegalStateException("Stream is closed")

  /** Reads all remaining bytes. */
  def read(): Array[Byte] = {
    ensureOpen()
    val out = new java.io.ByteArrayOutputStream()
    val chunk = new Array[Byte](8192)
    var n = inputStream.read(chunk)
    while (n != -1) {
      out.write(chunk, 0, n)
      n = inputStream.read(chunk)
    }
    out.toByteArray
  }

  /** Reads exactly `len` bytes (or until EOF). */
  def read(len: Int): Array[Byte] = {
    ensureOpen()
    if (len <= 0) return Array.emptyByteArray

    val buffer = new Array[Byte](len)
    var total = 0
    while (total < len) {
      val n = inputStream.read(buffer, total, len - total)
      if (n == -1) return if (total == 0) Array.emptyByteArray else buffer.take(total)
      total += n
    }
    buffer
  }

  override def close(): Unit = if (!closed) { closed = true; inputStream.close() }
  def isClosed: Boolean = closed
}

/** Manages the lifecycle of large objects (>2GB) stored in S3. */
object BigObjectManager extends LazyLogging {
  private val DEFAULT_BUCKET = "texera-big-objects"
  private lazy val db = SqlServer.getInstance().createDSLContext()

  /** Creates a big object from InputStream, uploads to S3, and registers in database. */
  def create(stream: InputStream, executionId: Int, operatorId: String): BigObjectPointer = {

    S3StorageClient.createBucketIfNotExist(DEFAULT_BUCKET)

    val objectKey = s"${System.currentTimeMillis()}/${UUID.randomUUID()}"
    val uri = s"s3://$DEFAULT_BUCKET/$objectKey"

    S3StorageClient.uploadObject(DEFAULT_BUCKET, objectKey, stream)

    try {
      db.insertInto(BIG_OBJECT)
        .columns(BIG_OBJECT.EXECUTION_ID, BIG_OBJECT.OPERATOR_ID, BIG_OBJECT.URI)
        .values(Int.box(executionId), operatorId, uri)
        .execute()
      logger.debug(s"Created big object: eid=$executionId, opid=$operatorId, uri=$uri")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register, cleaning up: $uri", e)
        try S3StorageClient.deleteObject(DEFAULT_BUCKET, objectKey)
        catch { case _: Exception => }
        throw new RuntimeException(s"Failed to create big object: ${e.getMessage}", e)
    }

    new BigObjectPointer(uri)
  }

  /** Opens a big object for reading. */
  def open(ptr: BigObjectPointer): BigObjectStream = {
    require(
      S3StorageClient.objectExists(ptr.getBucketName, ptr.getObjectKey),
      s"Big object does not exist: ${ptr.getUri}"
    )
    new BigObjectStream(S3StorageClient.downloadObject(ptr.getBucketName, ptr.getObjectKey))
  }

  /** Deletes all big objects associated with an execution ID. */
  def delete(executionId: Int): Unit = {
    val uris = db
      .select(BIG_OBJECT.URI)
      .from(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(executionId))
      .fetchInto(classOf[String])
      .asScala
      .toList

    if (uris.isEmpty) return logger.debug(s"No big objects for execution $executionId")

    logger.info(s"Deleting ${uris.size} big object(s) for execution $executionId")

    uris.foreach { uri =>
      try {
        val ptr = new BigObjectPointer(uri)
        S3StorageClient.deleteObject(ptr.getBucketName, ptr.getObjectKey)
      } catch {
        case e: Exception => logger.error(s"Failed to delete: $uri", e)
      }
    }

    db.deleteFrom(BIG_OBJECT).where(BIG_OBJECT.EXECUTION_ID.eq(executionId)).execute()
  }
}

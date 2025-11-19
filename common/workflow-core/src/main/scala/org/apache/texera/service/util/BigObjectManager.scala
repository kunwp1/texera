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
import org.apache.amber.core.executor.OperatorExecutor
import org.apache.amber.core.tuple.BigObject
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.BIG_OBJECT

import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * Manages the lifecycle of BigObjects stored in S3.
  *
  * Handles creation, tracking, and cleanup of large objects that exceed
  * normal tuple size limits. Objects are automatically cleaned up when
  * their associated workflow execution completes.
  */
object BigObjectManager extends LazyLogging {
  private val DEFAULT_BUCKET = "texera-big-objects"
  private lazy val db = SqlServer.getInstance().createDSLContext()

  /**
    * Creates a new BigObject reference and registers it for tracking.
    * The actual data upload happens separately via BigObjectOutputStream.
    *
    * @param executor The operator executor providing execution context
    * @return S3 URI string for the new BigObject (format: s3://bucket/key)
    * @throws RuntimeException if database registration fails
    */
  def create(executor: OperatorExecutor): String = {
    S3StorageClient.createBucketIfNotExist(DEFAULT_BUCKET)

    val objectKey = s"${System.currentTimeMillis()}/${UUID.randomUUID()}"
    val uri = s"s3://$DEFAULT_BUCKET/$objectKey"

    try {
      db.insertInto(BIG_OBJECT)
        .columns(BIG_OBJECT.EXECUTION_ID, BIG_OBJECT.OPERATOR_ID, BIG_OBJECT.URI)
        .values(Int.box(executor.executionId), executor.operatorId, uri)
        .execute()

      logger.debug(
        s"Created BigObject: eid=${executor.executionId}, opid=${executor.operatorId}, uri=$uri"
      )
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to register BigObject in database: ${e.getMessage}", e)
    }

    uri
  }

  /**
    * Deletes all BigObjects associated with an execution.
    * Removes both the S3 objects and database records.
    *
    * @param executionId The execution ID whose BigObjects should be deleted
    */
  def delete(executionId: Int): Unit = {
    val uris = db
      .select(BIG_OBJECT.URI)
      .from(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(executionId))
      .fetchInto(classOf[String])
      .asScala
      .toList

    if (uris.isEmpty) {
      logger.debug(s"No BigObjects found for execution $executionId")
      return
    }

    logger.info(s"Deleting ${uris.size} BigObject(s) for execution $executionId")

    uris.foreach { uri =>
      try {
        val bigObject = new BigObject(uri)
        S3StorageClient.deleteObject(bigObject.getBucketName, bigObject.getObjectKey)
      } catch {
        case e: Exception => logger.error(s"Failed to delete BigObject from S3: $uri", e)
      }
    }

    db.deleteFrom(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(executionId))
      .execute()
  }
}

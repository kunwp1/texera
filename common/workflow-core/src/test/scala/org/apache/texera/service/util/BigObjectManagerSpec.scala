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

import org.apache.amber.core.tuple.BigObjectPointer
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayInputStream

class BigObjectManagerSpec
    extends AnyFunSuite
    with MockTexeraDB
    with S3StorageTestBase
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  override def afterAll(): Unit = {
    shutdownDB()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    getDSLContext.deleteFrom(BIG_OBJECT).execute()
  }

  /** Creates mock workflow execution records needed for foreign key constraints. */
  private def createMockExecution(executionId: Int): Unit = {
    val dsl = getDSLContext
    val id = Int.box(executionId)

    dsl
      .insertInto(USER)
      .columns(USER.UID, USER.NAME, USER.EMAIL, USER.PASSWORD)
      .values(id, s"test_user_$executionId", s"test$executionId@test.com", "password")
      .onConflictDoNothing()
      .execute()

    dsl
      .insertInto(WORKFLOW)
      .columns(WORKFLOW.WID, WORKFLOW.NAME, WORKFLOW.CONTENT)
      .values(id, s"test_workflow_$executionId", "{}")
      .onConflictDoNothing()
      .execute()

    dsl
      .insertInto(WORKFLOW_OF_USER)
      .columns(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
      .values(id, id)
      .onConflictDoNothing()
      .execute()

    dsl
      .insertInto(WORKFLOW_VERSION)
      .columns(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.WID, WORKFLOW_VERSION.CONTENT)
      .values(id, id, "{}")
      .onConflictDoNothing()
      .execute()

    dsl
      .insertInto(WORKFLOW_EXECUTIONS)
      .columns(
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.VID,
        WORKFLOW_EXECUTIONS.UID,
        WORKFLOW_EXECUTIONS.STATUS,
        WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION
      )
      .values(id, id, id, Short.box(1.toShort), "test")
      .onConflictDoNothing()
      .execute()
  }

  /** Creates a big object from string data and returns its pointer. */
  private def createBigObject(
      data: String,
      execId: Int,
      opId: String = "test-op"
  ): BigObjectPointer = {
    createMockExecution(execId)
    BigObjectManager.create(new ByteArrayInputStream(data.getBytes), execId, opId)
  }

  /** Creates a BigObjectStream from test data. */
  private def createStream(data: String): BigObjectStream =
    new BigObjectStream(new ByteArrayInputStream(data.getBytes))

  /** Verifies that an object exists in S3. */
  private def assertObjectExists(pointer: BigObjectPointer, shouldExist: Boolean = true): Unit = {
    val exists = S3StorageClient.objectExists(pointer.getBucketName, pointer.getObjectKey)
    assert(exists == shouldExist)
  }

  /** Verifies standard bucket name. */
  private def assertStandardBucket(pointer: BigObjectPointer): Unit = {
    assert(pointer.getBucketName == "texera-big-objects")
    assert(pointer.getUri.startsWith("s3://texera-big-objects/"))
  }

  // ========================================
  // BigObjectStream Tests
  // ========================================

  test("BigObjectStream should read all bytes from stream") {
    val data = "Hello, World! This is a test."
    val stream = createStream(data)

    assert(stream.read().sameElements(data.getBytes))
    stream.close()
  }

  test("BigObjectStream should read exact number of bytes") {
    val stream = createStream("0123456789ABCDEF")
    val result = stream.read(10)

    assert(result.length == 10)
    assert(result.sameElements("0123456789".getBytes))
    stream.close()
  }

  test("BigObjectStream should handle reading more bytes than available") {
    val data = "Short"
    val stream = createStream(data)
    val result = stream.read(100)

    assert(result.length == data.length)
    assert(result.sameElements(data.getBytes))
    stream.close()
  }

  test("BigObjectStream should return empty array for 0 or negative byte reads") {
    val stream = createStream("Test")
    assert(stream.read(0).isEmpty)
    assert(stream.read(-5).isEmpty)
    stream.close()
  }

  test("BigObjectStream should return empty array at EOF") {
    val stream = createStream("EOF")
    stream.read() // Read all data
    assert(stream.read(10).isEmpty)
    stream.close()
  }

  test("BigObjectStream should track closed state correctly") {
    val stream = createStream("test")
    assert(!stream.isClosed)
    stream.close()
    assert(stream.isClosed)
  }

  test("BigObjectStream should throw exception when reading from closed stream") {
    val stream = createStream("test")
    stream.close()

    assertThrows[IllegalStateException](stream.read())
    assertThrows[IllegalStateException](stream.read(10))
  }

  test("BigObjectStream should handle multiple close calls") {
    val stream = createStream("test")
    stream.close()
    stream.close() // Should not throw
    assert(stream.isClosed)
  }

  test("BigObjectStream should read large data correctly") {
    val largeData = Array.fill[Byte](20000)((scala.util.Random.nextInt(256) - 128).toByte)
    val stream = new BigObjectStream(new ByteArrayInputStream(largeData))

    val result = stream.read()
    assert(result.sameElements(largeData))
    stream.close()
  }

  // ========================================
  // BigObjectManager Tests
  // ========================================

  test("BigObjectManager should create and register a big object") {
    val pointer = createBigObject("Test big object data", execId = 1, opId = "operator-1")

    assertStandardBucket(pointer)

    val record = getDSLContext
      .selectFrom(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(1).and(BIG_OBJECT.OPERATOR_ID.eq("operator-1")))
      .fetchOne()

    assert(record != null)
    assert(record.getUri == pointer.getUri)
  }

  test("BigObjectManager should open and read a big object") {
    val data = "Hello from big object!"
    val pointer = createBigObject(data, execId = 2)

    val stream = BigObjectManager.open(pointer)
    val readData = stream.read()
    stream.close()

    assert(readData.sameElements(data.getBytes))
  }

  test("BigObjectManager should fail to open non-existent big object") {
    val fakePointer = new BigObjectPointer("s3://texera-big-objects/nonexistent/file")
    assertThrows[IllegalArgumentException](BigObjectManager.open(fakePointer))
  }

  test("BigObjectManager should delete big objects by execution ID") {
    val execId = 3
    createMockExecution(execId)

    val pointer1 =
      BigObjectManager.create(new ByteArrayInputStream("Object 1".getBytes), execId, "op-1")
    val pointer2 =
      BigObjectManager.create(new ByteArrayInputStream("Object 2".getBytes), execId, "op-2")

    assertObjectExists(pointer1)
    assertObjectExists(pointer2)

    BigObjectManager.delete(execId)

    assertObjectExists(pointer1, shouldExist = false)
    assertObjectExists(pointer2, shouldExist = false)
    assert(
      getDSLContext.selectFrom(BIG_OBJECT).where(BIG_OBJECT.EXECUTION_ID.eq(execId)).fetch().isEmpty
    )
  }

  test("BigObjectManager should handle delete with no objects gracefully") {
    BigObjectManager.delete(9999) // Should not throw exception
  }

  test("BigObjectManager should not delete objects from different executions") {
    val pointer1 = createBigObject("Test data", execId = 4)
    val pointer2 = createBigObject("Test data", execId = 5)

    BigObjectManager.delete(4)

    assertObjectExists(pointer1, shouldExist = false)
    assertObjectExists(pointer2, shouldExist = true)

    BigObjectManager.delete(5)
  }

  test("BigObjectManager should create bucket if it doesn't exist") {
    val pointer = createBigObject("Test bucket creation", execId = 6)

    assertStandardBucket(pointer)
    assertObjectExists(pointer)

    BigObjectManager.delete(6)
  }

  test("BigObjectManager should handle large objects correctly") {
    val largeData = Array.fill[Byte](6 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)
    createMockExecution(7)
    val pointer = BigObjectManager.create(new ByteArrayInputStream(largeData), 7, "large-op")

    val stream = BigObjectManager.open(pointer)
    val readData = stream.read()
    stream.close()

    assert(readData.sameElements(largeData))
    BigObjectManager.delete(7)
  }

  test("BigObjectManager should generate unique URIs for different objects") {
    createMockExecution(8)
    val data = new ByteArrayInputStream("Unique URI test".getBytes)
    val pointer1 = BigObjectManager.create(data, 8, "test-op")
    val pointer2 =
      BigObjectManager.create(new ByteArrayInputStream("Unique URI test".getBytes), 8, "test-op")

    assert(pointer1.getUri != pointer2.getUri)
    assert(pointer1.getObjectKey != pointer2.getObjectKey)

    BigObjectManager.delete(8)
  }

  test("BigObjectManager should handle multiple reads from the same big object") {
    val data = "Multiple reads test data"
    val pointer = createBigObject(data, execId = 9)

    val stream1 = BigObjectManager.open(pointer)
    val readData1 = stream1.read()
    stream1.close()

    val stream2 = BigObjectManager.open(pointer)
    val readData2 = stream2.read()
    stream2.close()

    assert(readData1.sameElements(data.getBytes))
    assert(readData2.sameElements(data.getBytes))

    BigObjectManager.delete(9)
  }

  test("BigObjectManager should properly parse bucket name and object key from pointer") {
    val pointer = createBigObject("URI parsing test", execId = 10)

    assertStandardBucket(pointer)
    assert(pointer.getObjectKey.nonEmpty)
    assert(!pointer.getObjectKey.startsWith("/"))

    BigObjectManager.delete(10)
  }
}

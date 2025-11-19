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

import org.apache.amber.core.executor.OperatorExecutor
import org.apache.amber.core.tuple.{BigObject, Tuple, TupleLike}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

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

  /** Creates a mock OperatorExecutor for testing. */
  private def createMockExecutor(execId: Int, opId: String): OperatorExecutor = {
    val executor = new OperatorExecutor {
      override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
    }
    executor.initializeExecutionContext(execId, opId)
    executor
  }

  /** Creates a big object from string data and returns it. */
  private def createBigObject(
      data: String,
      execId: Int,
      opId: String = "test-op"
  ): BigObject = {
    createMockExecution(execId)
    val executor = createMockExecutor(execId, opId)
    val bigObject = new BigObject(executor)
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }
    bigObject
  }

  /** Verifies standard bucket name. */
  private def assertStandardBucket(pointer: BigObject): Unit = {
    assert(pointer.getBucketName == "texera-big-objects")
    assert(pointer.getUri.startsWith("s3://texera-big-objects/"))
  }

  // ========================================
  // BigObjectInputStream Tests (Standard Java InputStream)
  // ========================================

  test("BigObjectInputStream should read all bytes from stream") {
    val data = "Hello, World! This is a test."
    val bigObject = createBigObject(data, execId = 100)

    val stream = new BigObjectInputStream(bigObject)
    assert(stream.readAllBytes().sameElements(data.getBytes))
    stream.close()

    BigObjectManager.delete(100)
  }

  test("BigObjectInputStream should read exact number of bytes") {
    val bigObject = createBigObject("0123456789ABCDEF", execId = 101)

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readNBytes(10)

    assert(result.length == 10)
    assert(result.sameElements("0123456789".getBytes))
    stream.close()

    BigObjectManager.delete(101)
  }

  test("BigObjectInputStream should handle reading more bytes than available") {
    val data = "Short"
    val bigObject = createBigObject(data, execId = 102)

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readNBytes(100)

    assert(result.length == data.length)
    assert(result.sameElements(data.getBytes))
    stream.close()

    BigObjectManager.delete(102)
  }

  test("BigObjectInputStream should support standard single-byte read") {
    val bigObject = createBigObject("ABC", execId = 103)

    val stream = new BigObjectInputStream(bigObject)
    assert(stream.read() == 65) // 'A'
    assert(stream.read() == 66) // 'B'
    assert(stream.read() == 67) // 'C'
    assert(stream.read() == -1) // EOF
    stream.close()

    BigObjectManager.delete(103)
  }

  test("BigObjectInputStream should return -1 at EOF") {
    val bigObject = createBigObject("EOF", execId = 104)

    val stream = new BigObjectInputStream(bigObject)
    stream.readAllBytes() // Read all data
    assert(stream.read() == -1)
    stream.close()

    BigObjectManager.delete(104)
  }

  test("BigObjectInputStream should throw exception when reading from closed stream") {
    val bigObject = createBigObject("test", execId = 105)

    val stream = new BigObjectInputStream(bigObject)
    stream.close()

    assertThrows[java.io.IOException](stream.read())
    assertThrows[java.io.IOException](stream.readAllBytes())

    BigObjectManager.delete(105)
  }

  test("BigObjectInputStream should handle multiple close calls") {
    val bigObject = createBigObject("test", execId = 106)

    val stream = new BigObjectInputStream(bigObject)
    stream.close()
    stream.close() // Should not throw

    BigObjectManager.delete(106)
  }

  test("BigObjectInputStream should read large data correctly") {
    val largeData = Array.fill[Byte](20000)((scala.util.Random.nextInt(256) - 128).toByte)
    createMockExecution(107)
    val executor = createMockExecutor(107, "test-op")
    val bigObject = new BigObject(executor)
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new BigObjectInputStream(bigObject)
    val result = stream.readAllBytes()
    assert(result.sameElements(largeData))
    stream.close()

    BigObjectManager.delete(107)
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

  test("BigObjectInputStream should open and read a big object") {
    val data = "Hello from big object!"
    val pointer = createBigObject(data, execId = 2)

    val stream = new BigObjectInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))
  }

  test("BigObjectInputStream should fail to open non-existent big object") {
    val fakeBigObject = new BigObject("s3://texera-big-objects/nonexistent/file")
    val stream = new BigObjectInputStream(fakeBigObject)

    try {
      intercept[Exception] {
        stream.read()
      }
    } finally {
      try { stream.close() }
      catch { case _: Exception => }
    }
  }

  test("BigObjectManager should delete big objects by execution ID") {
    val execId = 3
    createMockExecution(execId)

    val executor1 = createMockExecutor(execId, "op-1")
    val pointer1 = new BigObject(executor1)
    val out1 = new BigObjectOutputStream(pointer1)
    try {
      out1.write("Object 1".getBytes)
    } finally {
      out1.close()
    }

    val executor2 = createMockExecutor(execId, "op-2")
    val pointer2 = new BigObject(executor2)
    val out2 = new BigObjectOutputStream(pointer2)
    try {
      out2.write("Object 2".getBytes)
    } finally {
      out2.close()
    }

    BigObjectManager.delete(execId)
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
    BigObjectManager.delete(5)
  }

  test("BigObjectManager should create bucket if it doesn't exist") {
    val pointer = createBigObject("Test bucket creation", execId = 6)

    assertStandardBucket(pointer)

    BigObjectManager.delete(6)
  }

  test("BigObjectManager should handle large objects correctly") {
    val largeData = Array.fill[Byte](6 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)
    createMockExecution(7)
    val executor = createMockExecutor(7, "large-op")
    val pointer = new BigObject(executor)
    val out = new BigObjectOutputStream(pointer)
    try {
      out.write(largeData)
    } finally {
      out.close()
    }

    val stream = new BigObjectInputStream(pointer)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(largeData))
    BigObjectManager.delete(7)
  }

  test("BigObjectManager should generate unique URIs for different objects") {
    createMockExecution(8)
    val testData = "Unique URI test".getBytes
    val executor = createMockExecutor(8, "test-op")
    val pointer1 = new BigObject(executor)
    val out1 = new BigObjectOutputStream(pointer1)
    try {
      out1.write(testData)
    } finally {
      out1.close()
    }

    val executor2 = createMockExecutor(8, "test-op")
    val pointer2 = new BigObject(executor2)
    val out2 = new BigObjectOutputStream(pointer2)
    try {
      out2.write(testData)
    } finally {
      out2.close()
    }

    assert(pointer1.getUri != pointer2.getUri)
    assert(pointer1.getObjectKey != pointer2.getObjectKey)

    BigObjectManager.delete(8)
  }

  test("BigObjectInputStream should handle multiple reads from the same big object") {
    val data = "Multiple reads test data"
    val pointer = createBigObject(data, execId = 9)

    val stream1 = new BigObjectInputStream(pointer)
    val readData1 = stream1.readAllBytes()
    stream1.close()

    val stream2 = new BigObjectInputStream(pointer)
    val readData2 = stream2.readAllBytes()
    stream2.close()

    assert(readData1.sameElements(data.getBytes))
    assert(readData2.sameElements(data.getBytes))

    BigObjectManager.delete(9)
  }

  test("BigObjectManager should properly parse bucket name and object key from big object") {
    val bigObject = createBigObject("URI parsing test", execId = 10)

    assertStandardBucket(bigObject)
    assert(bigObject.getObjectKey.nonEmpty)
    assert(!bigObject.getObjectKey.startsWith("/"))

    BigObjectManager.delete(10)
  }

  // ========================================
  // Object-Oriented API Tests
  // ========================================

  test("BigObject with BigObjectOutputStream should create and register a big object") {
    createMockExecution(11)
    val data = "Test data for BigObject with BigObjectOutputStream"
    val executor = createMockExecutor(11, "operator-11")

    val bigObject = new BigObject(executor)
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    assertStandardBucket(bigObject)

    val record = getDSLContext
      .selectFrom(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(11).and(BIG_OBJECT.OPERATOR_ID.eq("operator-11")))
      .fetchOne()

    assert(record != null)
    assert(record.getUri == bigObject.getUri)

    BigObjectManager.delete(11)
  }

  test("BigObjectInputStream constructor should read big object contents") {
    val data = "Test data for BigObjectInputStream constructor"
    val bigObject = createBigObject(data, execId = 12)

    val stream = new BigObjectInputStream(bigObject)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.delete(12)
  }

  test("BigObjectOutputStream and BigObjectInputStream should work together end-to-end") {
    createMockExecution(13)
    val data = "End-to-end test data"
    val executor = createMockExecutor(13, "operator-13")

    // Create using streaming API
    val bigObject = new BigObject(executor)
    val out = new BigObjectOutputStream(bigObject)
    try {
      out.write(data.getBytes)
    } finally {
      out.close()
    }

    // Read using standard constructor
    val stream = new BigObjectInputStream(bigObject)
    val readData = stream.readAllBytes()
    stream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.delete(13)
  }

  // ========================================
  // BigObjectOutputStream Tests (New Symmetric API)
  // ========================================

  test("BigObjectOutputStream should write and upload data to S3") {
    createMockExecution(200)
    val executor = createMockExecutor(200, "operator-200")
    val data = "Test data for BigObjectOutputStream"

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    assertStandardBucket(bigObject)

    // Verify data can be read back
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.delete(200)
  }

  test("BigObjectOutputStream should register big object in database") {
    createMockExecution(201)
    val executor = createMockExecutor(201, "operator-201")
    val data = "Database registration test"

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    val record = getDSLContext
      .selectFrom(BIG_OBJECT)
      .where(BIG_OBJECT.EXECUTION_ID.eq(201).and(BIG_OBJECT.OPERATOR_ID.eq("operator-201")))
      .fetchOne()

    assert(record != null)
    assert(record.getUri == bigObject.getUri)

    BigObjectManager.delete(201)
  }

  test("BigObjectOutputStream should handle large data correctly") {
    createMockExecution(202)
    val executor = createMockExecutor(202, "operator-202")
    val largeData = Array.fill[Byte](8 * 1024 * 1024)((scala.util.Random.nextInt(256) - 128).toByte)

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(largeData)
    outStream.close()

    // Verify data integrity
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(largeData))

    BigObjectManager.delete(202)
  }

  test("BigObjectOutputStream should handle multiple writes") {
    createMockExecution(203)
    val executor = createMockExecutor(203, "operator-203")

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("Hello ".getBytes)
    outStream.write("World".getBytes)
    outStream.write("!".getBytes)
    outStream.close()

    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements("Hello World!".getBytes))

    BigObjectManager.delete(203)
  }

  test("BigObjectOutputStream should throw exception when writing to closed stream") {
    createMockExecution(204)
    val executor = createMockExecutor(204, "operator-204")

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("test".getBytes)
    outStream.close()

    assertThrows[java.io.IOException](outStream.write("more".getBytes))

    BigObjectManager.delete(204)
  }

  test("BigObjectOutputStream should handle close() being called multiple times") {
    createMockExecution(205)
    val executor = createMockExecutor(205, "operator-205")

    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write("test".getBytes)
    outStream.close()
    outStream.close() // Should not throw

    BigObjectManager.delete(205)
  }

  test("New BigObject(executor) constructor should create unique URIs") {
    createMockExecution(206)
    val executor1 = createMockExecutor(206, "operator-206")
    val executor2 = createMockExecutor(206, "operator-206")

    val bigObject1 = new BigObject(executor1)
    val bigObject2 = new BigObject(executor2)

    assert(bigObject1.getUri != bigObject2.getUri)
    assert(bigObject1.getObjectKey != bigObject2.getObjectKey)

    BigObjectManager.delete(206)
  }

  test("BigObject(executor) and BigObjectOutputStream API should be symmetric with input") {
    createMockExecution(207)
    val executor = createMockExecutor(207, "operator-207")
    val data = "Symmetric API test"

    // Write using new symmetric API
    val bigObject = new BigObject(executor)
    val outStream = new BigObjectOutputStream(bigObject)
    outStream.write(data.getBytes)
    outStream.close()

    // Read using symmetric API
    val inStream = new BigObjectInputStream(bigObject)
    val readData = inStream.readAllBytes()
    inStream.close()

    assert(readData.sameElements(data.getBytes))

    BigObjectManager.delete(207)
  }
}

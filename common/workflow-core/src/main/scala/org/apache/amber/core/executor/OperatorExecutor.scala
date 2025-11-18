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

package org.apache.amber.core.executor

import org.apache.amber.core.state.State
import org.apache.amber.core.tuple.{BigObjectPointer, Tuple, TupleLike}
import org.apache.amber.core.workflow.PortIdentity
import org.apache.texera.service.util.{BigObjectManager, BigObjectStream}

import java.io.InputStream

trait OperatorExecutor {

  // Execution context
  private var _executionId: Option[Int] = None
  private var _operatorId: Option[String] = None

  protected def executionId: Int =
    _executionId.getOrElse(throw new IllegalStateException("Execution context not initialized"))

  protected def operatorId: String =
    _operatorId.getOrElse(throw new IllegalStateException("Execution context not initialized"))

  final def initializeExecutionContext(execId: Int, opId: String): Unit = {
    _executionId = Some(execId)
    _operatorId = Some(opId)
  }

  /**
    * Creates a big object from an InputStream and returns a pointer to it.
    * This is a convenience method that automatically uses the operator's execution context.
    *
    * @param stream The input stream containing the data to store
    * @return A BigObjectPointer that can be stored in tuple fields
    */
  final def createBigObject(stream: InputStream): BigObjectPointer = {
    BigObjectManager.create(stream, executionId, operatorId)
  }

  /**
    * Opens a big object for reading from a pointer.
    * This is a convenience method that wraps BigObjectManager.open().
    *
    * @param pointer The pointer to the big object to open
    * @return A BigObjectStream for reading the object's contents
    */
  final def openBigObject(pointer: BigObjectPointer): BigObjectStream = {
    BigObjectManager.open(pointer)
  }

  def open(): Unit = {}

  def produceStateOnStart(port: Int): Option[State] = None

  def processState(state: State, port: Int): Option[State] = {
    if (state.isPassToAllDownstream) {
      Some(state)
    } else {
      None
    }
  }

  def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    processTuple(tuple, port).map(t => (t, None))
  }

  def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike]

  def produceStateOnFinish(port: Int): Option[State] = None

  def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    onFinish(port).map(t => (t, None))
  }

  def onFinish(port: Int): Iterator[TupleLike] = Iterator.empty

  def close(): Unit = {}

}

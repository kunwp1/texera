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

package org.apache.texera.amber.operator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.texera.amber.core.offload.{OffloadConfig, SizingMode}
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Tests the offload declaration on a logical operator: every operator type can
  * be marked for offloading, and the marking survives the workflow JSON.
  */
class LogicalOpOffloadSpec extends AnyFlatSpec {

  private val mapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  // ---------------------------------------------------------------------------
  // Default state -- offloading is opt-in
  // ---------------------------------------------------------------------------

  "A LogicalOp" should "not be offloaded by default" in {
    val op = new SpecializedFilterOpDesc()
    assert(!op.offload.enabled)
    assert(!op.isOffloaded)
  }

  it should "become offloaded once a config with enabled=true is set" in {
    val op = new SpecializedFilterOpDesc()
    op.offload = OffloadConfig(enabled = true, instanceType = Some("m5.large"))
    assert(op.isOffloaded)
    assert(op.offload.instanceType.contains("m5.large"))
  }

  it should "stay offloaded in Advised mode, where the platform picks the size" in {
    val op = new SpecializedFilterOpDesc()
    op.offload = OffloadConfig(enabled = true, sizingMode = SizingMode.ADVISED)
    assert(op.isOffloaded)
    assert(op.offload.instanceType.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Workflow JSON round trip
  // ---------------------------------------------------------------------------

  it should "serialize its offload block into the operator JSON" in {
    val op = new SpecializedFilterOpDesc()
    op.offload = OffloadConfig(enabled = true, instanceType = Some("r5.xlarge"))
    val json = mapper.writeValueAsString(op)
    assert(json.contains("\"offload\""))
    assert(json.contains("r5.xlarge"))
  }

  it should "restore the offload block from operator JSON" in {
    val op = new SpecializedFilterOpDesc()
    op.offload = OffloadConfig(
      enabled = true,
      instanceType = Some("r5.xlarge"),
      sizingMode = SizingMode.ADVISED,
      safetyFactor = Some(1.75)
    )
    val restored =
      mapper.readValue(mapper.writeValueAsString(op), classOf[SpecializedFilterOpDesc])
    assert(restored.offload == op.offload)
  }

  // ---------------------------------------------------------------------------
  // Backward compatibility -- workflows saved before this feature existed
  // ---------------------------------------------------------------------------

  it should "default to not-offloaded when the JSON has no offload block" in {
    // Shape of a workflow saved before offloading existed: operatorType is the
    // polymorphic discriminator every persisted operator carries.
    val json = """{"operatorType":"Filter","operatorID":"Filter-1","predicates":[]}"""
    val restored = mapper.readValue(json, classOf[LogicalOp])
    assert(!restored.isOffloaded)
    assert(restored.offload == OffloadConfig())
  }

  it should "treat an explicit null offload block as not-offloaded" in {
    val json =
      """{"operatorType":"Filter","operatorID":"Filter-1","predicates":[],"offload":null}"""
    val restored = mapper.readValue(json, classOf[LogicalOp])
    assert(!restored.isOffloaded)
  }
}

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

package org.apache.texera.amber.core.offload

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Tests the per-operator offload declaration: whether an operator is offloaded,
  * and how its instance is chosen.
  *
  * This type is part of the workflow JSON the frontend sends, so it must
  * round-trip through Jackson and tolerate absent or unknown fields -- an older
  * saved workflow has no offload block at all.
  */
class OffloadConfigSpec extends AnyFlatSpec {

  private val mapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  // ---------------------------------------------------------------------------
  // Defaults -- offloading must be opt-in
  // ---------------------------------------------------------------------------

  "OffloadConfig" should "default to disabled so existing workflows are unaffected" in {
    val cfg = OffloadConfig()
    assert(!cfg.enabled)
    assert(cfg.instanceType.isEmpty)
    assert(cfg.sizingMode == SizingMode.MANUAL)
  }

  it should "not be considered offloaded when disabled, even with an instance set" in {
    val cfg = OffloadConfig(enabled = false, instanceType = Some("m5.large"))
    assert(!cfg.isOffloaded)
  }

  it should "be considered offloaded when enabled" in {
    assert(OffloadConfig(enabled = true, instanceType = Some("m5.large")).isOffloaded)
  }

  // ---------------------------------------------------------------------------
  // Validation (negative)
  // ---------------------------------------------------------------------------

  it should "reject a blank instance type when one is supplied" in {
    assertThrows[IllegalArgumentException](OffloadConfig(enabled = true, instanceType = Some("")))
    assertThrows[IllegalArgumentException](
      OffloadConfig(enabled = true, instanceType = Some("   "))
    )
  }

  it should "require an instance type when enabled in Manual mode" in {
    // Manual mode means the user picked the size; with nothing picked there is
    // no size to rent, so this must fail at validation rather than at run time.
    val cfg = OffloadConfig(enabled = true, instanceType = None, sizingMode = SizingMode.MANUAL)
    assert(cfg.validationError.isDefined)
  }

  it should "not require an instance type when enabled in Advised mode" in {
    val cfg = OffloadConfig(enabled = true, instanceType = None, sizingMode = SizingMode.ADVISED)
    assert(cfg.validationError.isEmpty)
  }

  it should "report no validation error when disabled regardless of other fields" in {
    assert(OffloadConfig(enabled = false).validationError.isEmpty)
    assert(
      OffloadConfig(enabled = false, sizingMode = SizingMode.MANUAL).validationError.isEmpty
    )
  }

  it should "reject a safety factor below 1.0" in {
    assertThrows[IllegalArgumentException](OffloadConfig(safetyFactor = Some(0.5)))
  }

  it should "accept a safety factor of exactly 1.0 and above" in {
    assert(OffloadConfig(safetyFactor = Some(1.0)).safetyFactor.contains(1.0))
    assert(OffloadConfig(safetyFactor = Some(2.5)).safetyFactor.contains(2.5))
  }

  it should "leave safetyFactor unset by default so the config default applies" in {
    // A hardcoded per-operator default would shadow offload.default-safety-factor
    // and make that knob dead.
    assert(OffloadConfig().safetyFactor.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // JSON round trip -- the config arrives inside the workflow document
  // ---------------------------------------------------------------------------

  it should "round trip through JSON with all fields set" in {
    val cfg = OffloadConfig(
      enabled = true,
      instanceType = Some("m5.large"),
      sizingMode = SizingMode.ADVISED,
      safetyFactor = Some(1.5)
    )
    val restored = mapper.readValue(mapper.writeValueAsString(cfg), classOf[OffloadConfig])
    assert(restored == cfg)
  }

  it should "deserialize from an empty JSON object using defaults" in {
    val restored = mapper.readValue("{}", classOf[OffloadConfig])
    assert(restored == OffloadConfig())
  }

  it should "still accept a scalar safetyFactor from a workflow saved before it became optional" in {
    // safetyFactor was a plain Double before it became Option[Double]; a stored
    // workflow carrying the scalar must keep deserializing rather than failing
    // the whole document.
    val json =
      """{"enabled":true,"instanceType":"m5.large","sizingMode":"Manual","safetyFactor":1.25}"""
    val restored = mapper.readValue(json, classOf[OffloadConfig])
    assert(restored.safetyFactor.contains(1.25))
    assert(restored.enabled)
  }

  it should "treat an explicitly null safetyFactor as unset" in {
    val restored =
      mapper.readValue("""{"enabled":true,"safetyFactor":null}""", classOf[OffloadConfig])
    assert(restored.safetyFactor.isEmpty)
  }

  it should "ignore unknown fields, so a newer UI cannot break an older engine" in {
    val json = """{"enabled":true,"instanceType":"m5.large","futureField":42}"""
    val restored = mapper.readValue(json, classOf[OffloadConfig])
    assert(restored.enabled)
    assert(restored.instanceType.contains("m5.large"))
  }

  it should "reject malformed JSON for the sizing mode" in {
    assertThrows[Exception] {
      mapper.readValue("""{"sizingMode":"NotAMode"}""", classOf[OffloadConfig])
    }
  }

  // ---------------------------------------------------------------------------
  // SizingMode
  // ---------------------------------------------------------------------------

  "SizingMode" should "parse its known names case-insensitively" in {
    assert(SizingMode.fromString("manual") == SizingMode.MANUAL)
    assert(SizingMode.fromString("ADVISED") == SizingMode.ADVISED)
    assert(SizingMode.fromString("  Advised  ") == SizingMode.ADVISED)
  }

  it should "reject an unknown, empty, or null name rather than defaulting" in {
    // Defaulting would silently turn an intended Advised sizing into a Manual
    // one with no instance selected, failing only later at provisioning time.
    assertThrows[IllegalArgumentException](SizingMode.fromString("turbo"))
    assertThrows[IllegalArgumentException](SizingMode.fromString(""))
    assertThrows[IllegalArgumentException](SizingMode.fromString(null))
  }

  it should "serialize as its display name, not its Java constant name" in {
    assert(mapper.writeValueAsString(SizingMode.ADVISED) == "\"Advised\"")
    assert(SizingMode.MANUAL.toString == "Manual")
  }
}

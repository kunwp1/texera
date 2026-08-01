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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[OffloadConfigSettings]]. Reading each value forces resolution from
  * offload.conf, so a renamed or mistyped key surfaces here as a
  * ConfigException. Values carrying a `${?ENV}` override are guarded.
  */
class OffloadConfigSettingsSpec extends AnyFlatSpec with Matchers {

  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "OffloadConfigSettings" should "resolve its scalar settings from offload.conf" in {
    // Off by default: offloading rents billable resources, so it must be opt-in.
    ifUnset("OFFLOAD_ENABLED")(OffloadConfigSettings.enabled shouldBe false)
    ifUnset("OFFLOAD_PROVIDER")(OffloadConfigSettings.provider shouldBe "docker")
    ifUnset("OFFLOAD_DOCKER_IMAGE")(
      OffloadConfigSettings.dockerImage shouldBe "texera-computing-unit-worker:latest"
    )
    ifUnset("OFFLOAD_DOCKER_BINARY")(OffloadConfigSettings.dockerBinary shouldBe "docker")
    ifUnset("OFFLOAD_MAX_INSTANCE_LIFETIME_MINUTES")(
      OffloadConfigSettings.maxInstanceLifetimeMinutes shouldBe 60
    )
    ifUnset("OFFLOAD_JOIN_TIMEOUT_SECONDS")(
      OffloadConfigSettings.joinTimeoutSeconds shouldBe 300
    )
    ifUnset("OFFLOAD_JOIN_POLL_INTERVAL_MS")(
      OffloadConfigSettings.joinPollIntervalMs shouldBe 1000
    )
    ifUnset("OFFLOAD_DEFAULT_SAFETY_FACTOR")(
      OffloadConfigSettings.defaultSafetyFactor shouldBe 1.25
    )
  }

  it should "build a non-empty instance catalog" in {
    OffloadConfigSettings.catalog.instances should not be empty
  }

  it should "parse every catalog entry with positive memory and non-negative price" in {
    OffloadConfigSettings.catalog.instances.foreach { i =>
      withClue(s"instance ${i.name}: ") {
        i.name.trim should not be empty
        i.vcpu should be > 0
        i.memoryGiB should be > 0.0
        i.pricePerHour should be >= 0.0
      }
    }
  }

  it should "include the free local stand-in instances used by the local provider" in {
    OffloadConfigSettings.catalog.byName("local-2g") should not be empty
    OffloadConfigSettings.catalog.byName("local-2g").get.pricePerHour shouldBe 0.0
  }

  it should "include EC2 instance types for the cloud provider" in {
    OffloadConfigSettings.catalog.byName("m5.large") should not be empty
    OffloadConfigSettings.catalog.byName("r5.xlarge") should not be empty
  }

  it should "not contain duplicate instance names" in {
    val names = OffloadConfigSettings.catalog.instances.map(_.name)
    names.distinct.size shouldBe names.size
  }

  it should "select a cheapest fitting instance through the resolved catalog" in {
    // 3 GiB: must fit, must not pick something with less memory.
    val chosen = OffloadConfigSettings.catalog.cheapestFitting(3L * 1024 * 1024 * 1024)
    chosen should not be empty
    chosen.get.memoryBytes should be >= 3L * 1024 * 1024 * 1024
  }

  it should "expose a default safety factor usable as a headroom multiplier" in {
    // Below 1.0 would shrink an estimate and defeat the safety margin.
    OffloadConfigSettings.defaultSafetyFactor should be >= 1.0
  }
}

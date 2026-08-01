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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.texera.common.offload.{InstanceCatalog, InstanceType}

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Settings for per-operator offloading to elastic cloud compute, from
  * offload.conf.
  */
object OffloadConfigSettings {

  private val conf: Config = ConfigFactory.parseResources("offload.conf").resolve()

  val enabled: Boolean = conf.getBoolean("offload.enabled")

  val provider: String = conf.getString("offload.provider")

  val dockerImage: String = conf.getString("offload.docker-image")

  val dockerBinary: String = conf.getString("offload.docker-binary")

  val dockerNetwork: String = conf.getString("offload.docker-network")

  val coordinatorAdvertisedHostname: String =
    conf.getString("offload.coordinator-advertised-hostname")

  val workerPekkoBasePort: Int = conf.getInt("offload.worker-pekko-base-port")

  /** True when worker containers share the host's network namespace. */
  def usesHostNetwork: Boolean = dockerNetwork == "host"

  val maxInstanceLifetimeMinutes: Int = conf.getInt("offload.max-instance-lifetime-minutes")

  val joinTimeoutSeconds: Int = conf.getInt("offload.join-timeout-seconds")

  val joinPollIntervalMs: Int = conf.getInt("offload.join-poll-interval-ms")

  val defaultSafetyFactor: Double = conf.getDouble("offload.default-safety-factor")

  /** The instances the platform may rent for an offloaded operator. */
  val catalog: InstanceCatalog = InstanceCatalog(
    conf
      .getConfigList("offload.instances")
      .asScala
      .map(c =>
        InstanceType(
          name = c.getString("name"),
          vcpu = c.getInt("vcpu"),
          memoryGiB = c.getDouble("memory-gib"),
          pricePerHour = c.getDouble("price-per-hour")
        )
      )
      .toSeq
  )
}

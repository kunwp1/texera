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

package org.apache.texera.common.offload

/**
  * The contract for launching a Texera worker node, shared by every offload
  * provider regardless of how it starts the process (a container, a VM).
  *
  * A rented instance becomes usable by running this entry point, which joins the
  * Pekko cluster as a worker; placement then pins the offloaded operator to it.
  *
  * These identifiers live in common/config because that module cannot depend on
  * amber, where the worker actually lives. `OffloadWorkerEntryPointSpec` in amber
  * pins them to the real class and argument parser, so a rename fails the build
  * instead of killing a spawned worker the coordinator is already waiting on.
  */
object OffloadWorkerLauncher {

  /**
    * Texera's worker entry point, which calls AmberRuntime.startActorWorker to
    * join the cluster.
    */
  val WorkerMainClass: String = "org.apache.texera.web.ComputingUnitWorker"

  /**
    * Launcher script shipped in the worker image (sbt-native-packager), which
    * invokes [[WorkerMainClass]]. The Docker provider runs this rather than
    * `java` directly, so the image's classpath and JVM options apply.
    */
  val WorkerLauncherScript: String = "bin/computing-unit-worker"

  /** Flag the worker's argument parser expects for the seed node's address. */
  val SeedAddressFlag: String = "--serverAddr"

  /**
    * Env var marking a worker as dedicated to one offloaded operator.
    *
    * The worker turns this into a Pekko cluster role, which keeps general
    * round-robin placement off the node -- it is sized for exactly one operator.
    * An env var rather than a CLI flag because the worker's argument parser
    * rejects unknown flags, and `docker run -e` sets it naturally.
    *
    * Mirrors AmberConfig.DedicatedOffloadNodeEnv; OffloadWorkerEntryPointSpec in
    * amber pins the two together.
    */
  val DedicatedNodeEnv: String = "TEXERA_DEDICATED_OFFLOAD_NODE"

  /**
    * Env var telling the worker what hostname to advertise to cluster peers.
    *
    * A container's own view of itself ("localhost", or the host's public IP the
    * image's default discovery would find) is not routable from the coordinator,
    * so the provider states the reachable name explicitly.
    *
    * Mirrors AmberConfig.AdvertisedHostnameEnv; OffloadWorkerEntryPointSpec pins
    * the two together.
    */
  val AdvertisedHostnameEnv: String = "TEXERA_CLUSTER_ADVERTISED_HOSTNAME"

  /**
    * Env var pinning the worker's Pekko port.
    *
    * Fixed rather than ephemeral so the coordinator can reach the worker at a
    * predictable address, and so the port can be published on a bridge network.
    *
    * Mirrors AmberConfig.AdvertisedPortEnv.
    */
  val AdvertisedPortEnv: String = "TEXERA_CLUSTER_ADVERTISED_PORT"
}

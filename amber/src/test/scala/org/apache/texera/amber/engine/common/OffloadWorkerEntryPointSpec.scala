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

package org.apache.texera.amber.engine.common

import org.apache.texera.common.offload.OffloadWorkerLauncher
import org.apache.texera.web.ComputingUnitWorker
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Guards the contract between the offload provider and the worker entry point.
  *
  * OffloadWorkerLauncher names the worker class and its seed-address flag as strings,
  * because common/config cannot depend on amber. Nothing in that module can
  * notice if the class is renamed or the flag changes -- the spawned worker
  * would simply die with ClassNotFoundError or an unknown-argument error, on a
  * node the engine is already waiting to have join. These tests fail at build
  * time instead.
  */
class OffloadWorkerEntryPointSpec extends AnyFlatSpec {

  "the worker main class named by OffloadWorkerLauncher" should "exist on the classpath" in {
    val loaded = Class.forName(OffloadWorkerLauncher.WorkerMainClass + "$")
    assert(loaded != null)
  }

  it should "be the ComputingUnitWorker object" in {
    assert(
      OffloadWorkerLauncher.WorkerMainClass == ComputingUnitWorker.getClass.getName.stripSuffix("$")
    )
  }

  it should "expose a main method for java to launch" in {
    val loaded = Class.forName(OffloadWorkerLauncher.WorkerMainClass + "$")
    assert(loaded.getMethods.exists(_.getName == "main"))
  }

  "the seed-address flag" should "be one the worker's argument parser accepts" in {
    val parsed = ComputingUnitWorker.parseArgs(
      Array(OffloadWorkerLauncher.SeedAddressFlag, "10.0.0.1")
    )
    assert(parsed.get(Symbol("serverAddr")).contains("10.0.0.1"))
  }

  "the cluster-advertisement env vars" should "match the names the worker reads" in {
    // The provider sets these with `docker run -e`; the worker turns them into its
    // advertised Pekko address. A mismatch means the worker advertises an address
    // the coordinator cannot route to, so it joins a cluster of one and the
    // offload silently never happens -- the join just times out.
    assert(OffloadWorkerLauncher.AdvertisedHostnameEnv == AmberConfig.AdvertisedHostnameEnv)
    assert(OffloadWorkerLauncher.AdvertisedPortEnv == AmberConfig.AdvertisedPortEnv)
  }

  "the dedicated-node env var" should "match the name the worker reads" in {
    // The provider sets this with `docker run -e`; the worker turns it into the
    // Pekko role that keeps general placement off the node. A mismatch would
    // silently leave the rented node in the general pool, so another operator's
    // workers could land inside its memory cap.
    assert(OffloadWorkerLauncher.DedicatedNodeEnv == AmberConfig.DedicatedOffloadNodeEnv)
  }

  it should "be rejected by the parser if it ever changes shape" in {
    // Pins the negative direction: the parser is strict, so a bare positional
    // address (or any other flag) aborts the worker at startup.
    assertThrows[Exception](ComputingUnitWorker.parseArgs(Array("10.0.0.1")))
  }
}

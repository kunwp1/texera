/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.amber.engine.common

import org.apache.pekko.actor.Address

object AmberConfig {
  var masterNodeAddr: Address = Address("pekko", "Amber", "localhost", 2552)

  /** Env var set on a container rented for one offloaded operator. */
  val DedicatedOffloadNodeEnv: String = "TEXERA_DEDICATED_OFFLOAD_NODE"

  /**
    * Whether this JVM is a node rented for a single offloaded operator.
    *
    * Read from the environment rather than a CLI flag so the offload provider can
    * set it with `docker run -e`, without changing the worker's strict argument
    * parser.
    */
  def isDedicatedOffloadNode: Boolean =
    sys.env.get(DedicatedOffloadNodeEnv).exists(v => v == "1" || v.equalsIgnoreCase("true"))

  /** Env var overriding the hostname this node advertises to cluster peers. */
  val AdvertisedHostnameEnv: String = "TEXERA_CLUSTER_ADVERTISED_HOSTNAME"

  /** Env var pinning the Pekko port this node advertises and binds. */
  val AdvertisedPortEnv: String = "TEXERA_CLUSTER_ADVERTISED_PORT"

  /**
    * Hostname this node advertises to cluster peers, if overridden.
    *
    * Distinct from the bind address: a node always binds 0.0.0.0, but must
    * advertise a name its peers can route to. Those differ whenever a peer lives
    * in another network namespace -- an offloaded worker in a container reaching
    * the coordinator on the host, or the reverse.
    */
  def advertisedHostname: Option[String] =
    sys.env.get(AdvertisedHostnameEnv).map(_.trim).filter(_.nonEmpty)

  /**
    * Pekko port this node advertises and binds, if pinned.
    *
    * An offloaded worker needs a predictable port so the provider can publish it;
    * the default (ephemeral) is fine for co-located workers.
    */
  def advertisedPort: Option[Int] =
    sys.env
      .get(AdvertisedPortEnv)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(v =>
        try v.toInt
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"$AdvertisedPortEnv must be an integer port, got '$v'"
            )
        }
      )
}

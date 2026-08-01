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

import org.apache.pekko.actor.{ActorSystem, Address, Cancellable, DeadLetter, Props}
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.texera.amber.clustering.ClusterListener
import org.apache.texera.common.config.PekkoConfig
import org.apache.texera.amber.engine.architecture.messaginglayer.DeadLetterMonitorActor

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

object AmberRuntime {

  private var _serde: Serialization = _
  private var _actorSystem: ActorSystem = _

  def serde: Serialization = {
    if (_serde == null) {
      if (_actorSystem == null) {
        _serde = SerializationExtension(ActorSystem("Amber", pekkoConfig))
      } else {
        _serde = SerializationExtension(_actorSystem)
      }
    }
    _serde
  }

  def actorSystem: ActorSystem = {
    _actorSystem
  }

  def scheduleCallThroughActorSystem(delay: FiniteDuration)(call: => Unit): Cancellable = {
    _actorSystem.scheduler.scheduleOnce(delay)(call)
  }

  def scheduleRecurringCallThroughActorSystem(initialDelay: FiniteDuration, delay: FiniteDuration)(
      call: => Unit
  ): Cancellable = {
    _actorSystem.scheduler.scheduleWithFixedDelay(initialDelay, delay)(() => call)
  }

  private def getNodeIpAddress: String = {
    try {
      val query = new URL("http://checkip.amazonaws.com")
      val in = new BufferedReader(new InputStreamReader(query.openStream()))
      in.readLine()
    } catch {
      case e: Exception => throw e
    }
  }

  def startActorMaster(clusterMode: Boolean): Unit = {
    // What peers dial to reach this node, which is not necessarily what it binds
    // to. Offloaded workers run in their own network namespace, where "localhost"
    // is the worker itself -- so the coordinator has to advertise a name reachable
    // from outside. Explicit config wins; otherwise fall back to the old
    // behaviour (public IP in cluster mode, localhost when standalone).
    val advertisedHost = AmberConfig.advertisedHostname.getOrElse {
      if (clusterMode) getNodeIpAddress else "localhost"
    }

    // Bind on every interface so the node is reachable however a peer routes to
    // it (loopback for same-host clients, the Docker bridge for containers).
    // Binding only to the advertised name would make a container-reachable
    // address unreachable from the host, and vice versa.
    val masterConfig = ConfigFactory
      .parseString(s"""
        pekko.remote.artery.canonical.port = 2552
        pekko.remote.artery.canonical.hostname = $advertisedHost
        pekko.remote.artery.bind.hostname = "0.0.0.0"
        pekko.remote.artery.bind.port = 2552
        pekko.cluster.seed-nodes = [ "pekko://Amber@$advertisedHost:2552" ]
        """)
      .withFallback(pekkoConfig)
      .resolve()
    AmberConfig.masterNodeAddr = createMasterAddress(advertisedHost)
    createAmberSystem(masterConfig)
  }

  def pekkoConfig: Config = PekkoConfig.pekkoConfig

  private def createMasterAddress(addr: String): Address = Address("pekko", "Amber", addr, 2552)

  def startActorWorker(mainNodeAddress: Option[String]): Unit = {
    val addr = mainNodeAddress.getOrElse("localhost")
    // Inside a container the old getNodeIpAddress call returns the host's PUBLIC
    // IP (it asks an external echo service), which the coordinator cannot route
    // back to. An offloaded worker therefore advertises the name its coordinator
    // can actually reach -- set explicitly by the provider.
    val localIpAddress = AmberConfig.advertisedHostname.getOrElse {
      if (mainNodeAddress.isDefined) getNodeIpAddress else "localhost"
    }
    // A node rented for one offloaded operator declares the dedicated role, so
    // general round-robin placement can exclude it. Without that, another
    // operator's workers land inside the cgroup sized for the offloaded one --
    // which both invalidates its memory sizing and can get it OOM-killed.
    val roles =
      if (AmberConfig.isDedicatedOffloadNode) s"""pekko.cluster.roles = ["$DedicatedOffloadRole"]"""
      else ""
    // A fixed port when one is configured: an offloaded worker in its own network
    // namespace must be reachable at a port the coordinator can predict and
    // publish, which an ephemeral port (0) cannot provide.
    val port = AmberConfig.advertisedPort.getOrElse(0)
    val workerConfig = ConfigFactory
      .parseString(s"""
        pekko.remote.artery.canonical.hostname = $localIpAddress
        pekko.remote.artery.canonical.port = $port
        pekko.remote.artery.bind.hostname = "0.0.0.0"
        pekko.remote.artery.bind.port = $port
        pekko.cluster.seed-nodes = [ "pekko://Amber@$addr:2552" ]
        $roles
        """)
      .withFallback(pekkoConfig)
      .resolve()
    AmberConfig.masterNodeAddr = createMasterAddress(addr)
    createAmberSystem(workerConfig)
  }

  /**
    * Pekko cluster role marking a node rented for a single offloaded operator.
    *
    * Such a node is sized for exactly one operator, so it must not receive workers
    * from the general placement pool.
    */
  val DedicatedOffloadRole: String = "dedicated-offload"

  private def createAmberSystem(actorSystemConf: Config): Unit = {
    _actorSystem = ActorSystem("Amber", actorSystemConf)
    _actorSystem.actorOf(Props[ClusterListener](), "cluster-info")
    val deadLetterMonitorActor =
      _actorSystem.actorOf(Props[DeadLetterMonitorActor](), name = "dead-letter-monitor-actor")
    _actorSystem.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])
    _serde = SerializationExtension(_actorSystem)
  }
}

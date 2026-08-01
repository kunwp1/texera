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

package org.apache.texera.amber.engine.offload

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{Actor, ActorSystem, Address, Deploy, Identify, Props}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.pattern.{ask, gracefulStop}
import org.apache.pekko.remote.RemoteScope
import org.apache.pekko.util.Timeout
import org.apache.texera.common.config.PekkoConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * When `gracefulStop` on a REMOTELY deployed actor delivers `Terminated`, and when it
  * does not.
  *
  * ==Why this exists==
  *
  * This is the evidence behind `ClusterListener.placeableAddresses` refusing to publish a
  * member below `Up`. Without it that filter reads like an arbitrary restriction and is
  * an easy "simplification" to undo, so the constraint is pinned here rather than only
  * described in a comment.
  *
  * Region teardown calls `gracefulStop(workerRef, 5s)` on every worker
  * (`RegionExecutionManager.terminateWorkers`). Until operator offload every worker ran
  * on the coordinator itself, so the call was always local and `Terminated` never had to
  * cross a wire. The first offloaded runs timed out there --
  * `AskTimeoutException ... Message of type [PoisonPill$]` -- leaving the execution
  * wedged in RUNNING with a rented instance still up, even though thread dumps of the
  * worker showed its `postStop` had run to completion and the actor was gone.
  *
  * The difference turned out to be cluster membership: the coordinator deployed onto a
  * rented node as soon as it appeared in `cluster.state.members`, and with
  * `leader-actions-interval = 10s` the operator ran and finished inside the ~8s the node
  * spent `Joining`. Remote death watch delivers nothing for an actor on a member that is
  * not yet `Up`.
  *
  * The second case is a CHARACTERISATION of Pekko, not a defect this repo can fix -- it
  * asserts the failure, because that failure is the reason the filter exists. If it ever
  * stops holding, Pekko's behaviour changed and the wait for `Up` can be reconsidered.
  */
class RemoteGracefulStopSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Ports well away from a running local-dev stack (coordinator 2552, workers 2560+).
  private val CoordinatorPort = 27552
  private val WorkerPort = 27553
  private val JoiningPort = 27554

  private def systemConfig(port: Int, seedPort: Int) =
    ConfigFactory
      .parseString(s"""
        pekko.remote.artery.canonical.hostname = "127.0.0.1"
        pekko.remote.artery.canonical.port = $port
        pekko.remote.artery.bind.hostname = "0.0.0.0"
        pekko.remote.artery.bind.port = $port
        pekko.cluster.seed-nodes = [ "pekko://RemoteStopTest@127.0.0.1:$seedPort" ]
        """)
      .withFallback(PekkoConfig.pekkoConfig)
      .resolve()

  private var coordinator: ActorSystem = _
  private var worker: ActorSystem = _
  private var workerAddress: Address = _

  override def beforeAll(): Unit = {
    coordinator = ActorSystem("RemoteStopTest", systemConfig(CoordinatorPort, CoordinatorPort))
    worker = ActorSystem("RemoteStopTest", systemConfig(WorkerPort, CoordinatorPort))
    workerAddress = Cluster(worker).selfAddress

    // Both nodes Up before deploying, so placement is not racing membership.
    val deadline = 60.seconds.fromNow
    while (Cluster(coordinator).state.members.count(_.status.toString == "Up") < 2) {
      if (deadline.isOverdue()) fail("cluster did not converge to 2 Up members")
      Thread.sleep(200)
    }
  }

  override def afterAll(): Unit = {
    if (coordinator != null) Await.ready(coordinator.terminate(), 30.seconds)
    if (worker != null) Await.ready(worker.terminate(), 30.seconds)
  }

  "gracefulStop" should "stop a remotely deployed actor" in {
    val ref = coordinator.actorOf(
      Props[RemoteGracefulStopSpec.Quiet]().withDeploy(Deploy(scope = RemoteScope(workerAddress))),
      "quiet"
    )

    // The actor really is on the other node, and really is reachable -- otherwise a
    // gracefulStop failure below would be meaningless.
    implicit val timeout: Timeout = Timeout(10.seconds)
    Await.result(ref ? Identify(1), 10.seconds)
    ref.path.address.port shouldBe Some(WorkerPort)

    // The call region teardown makes. If remote DeathWatch is broken, this times out.
    Await.result(gracefulStop(ref, 5.seconds), 10.seconds) shouldBe true
  }

  it should "NOT stop a remote actor on a node that has not yet reached Up" in {
    // What the field actually does. A rented node is deployed onto the moment it appears
    // in the cluster, and `leader-actions-interval` is 10s, so the coordinator runs the
    // operator and tears it down while the node is still Joining. In executions 50 and 54
    // the gracefulStop watch was registered 1.4s and 0.7s respectively BEFORE the node
    // reached Up, and both timed out -- while the worker's own thread dumps show its
    // postStop ran to completion.
    //
    // The case above passes because beforeAll waits for two Up members, so it never
    // exercises this window.
    val joining = ActorSystem("RemoteStopTest", systemConfig(JoiningPort, CoordinatorPort))
    try {
      val addr = Cluster(joining).selfAddress
      val deadline = 60.seconds.fromNow
      while (!Cluster(coordinator).state.members.exists(_.address == addr)) {
        if (deadline.isOverdue()) fail("third node never appeared in cluster membership")
        Thread.sleep(50)
      }

      val statusAtWatch = Cluster(coordinator).state.members.find(_.address == addr).map(_.status)
      // If the leader promoted it before we got here the case proves nothing either way.
      assume(
        !statusAtWatch.contains(MemberStatus.Up),
        s"node reached Up too quickly to exercise the Joining window (status $statusAtWatch)"
      )

      implicit val timeout: Timeout = Timeout(10.seconds)

      val whileJoining = coordinator.actorOf(
        Props[RemoteGracefulStopSpec.Quiet]().withDeploy(Deploy(scope = RemoteScope(addr))),
        "quiet-joining"
      )
      // Reachable for ordinary messages: a full round trip to the actor succeeds. So the
      // association is up and this is specifically about the death watch.
      Await.result(whileJoining ? Identify(1), 10.seconds) shouldBe a[Any]

      val stoppedWhileJoining =
        try Await.result(gracefulStop(whileJoining, 5.seconds), 10.seconds)
        catch { case _: Exception => false }

      // Same node, same actor class, now Up. This is the control: if the stop below
      // succeeds, membership status is the only variable that differs.
      val upDeadline = 60.seconds.fromNow
      while (
        !Cluster(coordinator).state.members.exists { m =>
          m.address == addr && m.status == MemberStatus.Up
        }
      ) {
        if (upDeadline.isOverdue()) fail("third node never reached Up")
        Thread.sleep(100)
      }

      val whenUp = coordinator.actorOf(
        Props[RemoteGracefulStopSpec.Quiet]().withDeploy(Deploy(scope = RemoteScope(addr))),
        "quiet-up"
      )
      Await.result(whenUp ? Identify(1), 10.seconds)
      val stoppedWhenUp = Await.result(gracefulStop(whenUp, 5.seconds), 10.seconds)

      withClue(
        s"stopped while Joining = $stoppedWhileJoining, stopped once Up = $stoppedWhenUp. " +
          "If the Joining case now succeeds, Pekko's behaviour changed and " +
          "ClusterListener.placeableAddresses no longer has to wait for Up."
      ) {
        // The control: same node, same actor class, once the leader has promoted it.
        stoppedWhenUp shouldBe true
        // The constraint the offload path is built on. Not a bug we can fix here --
        // it is why `placeableAddresses` refuses to hand out a member below Up.
        stoppedWhileJoining shouldBe false
      }
    } finally {
      Await.ready(joining.terminate(), 30.seconds)
    }
  }

}

object RemoteGracefulStopSpec {

  /**
    * Deliberately trivial: no state, no lifecycle hooks, nothing that could delay its own
    * shutdown. Whether `gracefulStop` returns is then a property of the cluster, not of
    * the actor.
    */
  class Quiet extends Actor {
    override def receive: Receive = { case _ => () }
  }
}

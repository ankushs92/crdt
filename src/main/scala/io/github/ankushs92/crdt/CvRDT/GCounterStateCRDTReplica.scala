package io.github.ankushs92.crdt.CvRDT

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.CvRDT.payload.{GCounterPayload, IntVersionVector}
import io.github.ankushs92.crdt.StateCRDTReplica



case class GCounterStateCRDTReplica(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Int, GCounterPayload, Int] {

  private val versionVec = IntVersionVector(replicaCount)

  override def merge(state1: GCounterPayload, state2: GCounterPayload): GCounterPayload = {
    (0 until replicaCount).foreach { idx =>
      versionVec.modifyAtIndex(idx, Math.max(state1.verVec.atIndex(idx), state2.verVec.atIndex(idx)))
    }
    new GCounterPayload(versionVec)
  }

  override def getCurrentState = new GCounterPayload(versionVec)

  override def query: Int = versionVec.viewOverState.sum

  override def update(value: Int): Unit = versionVec.incrementBy(replicaId - 1, 1)

  override def getAddr: InetSocketAddress = addr
  override def getName: String = "gCounter"

}



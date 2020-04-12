package io.github.ankushs92.crdt.StateBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.StateCRDTReplica
import io.github.ankushs92.crdt.payload.{GCounterPayload, IntVersionVector}



case class GCounterStateCRDTReplica(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Int, GCounterPayload, Int] {

  private val versionVec = initialValue.verVec

  override def merge(replicaPayload: GCounterPayload): GCounterPayload = {
    (0 until replicaCount).foreach { idx =>
      versionVec.modifyAtIndex(idx, Math.max(versionVec.atIndex(idx), replicaPayload.verVec.atIndex(idx)))
    }
    new GCounterPayload(versionVec)
  }

  override def getCurrentState = new GCounterPayload(versionVec)

  override def query: Int = versionVec.viewOverState.sum

  override def update(value: Int): Unit = versionVec.incrementBy(replicaId - 1, 1)

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "gCounter"

  override def initialValue: GCounterPayload = new GCounterPayload(IntVersionVector(replicaCount))
}



package io.github.ankushs92.crdt.StateBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.StateCRDTReplica
import io.github.ankushs92.crdt.payload.{IntVersionVector, PNCounterPayload}

case class PNCounterStateCRDTReplica (replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Int, PNCounterPayload, Int] {

  private val incVerVec = initialValue.incVerVec
  private val decVerVec = initialValue.decVerVec

  override def update(write: Int): Unit = {
    if(write == -1) {
      decVerVec.incrementBy(replicaId - 1, 1)
    }
    else {
      incVerVec.incrementBy(replicaId - 1, 1)
    }
  }

  override def query: Int = {
   val increments = incVerVec.viewOverState.sum
   val decrements = decVerVec.viewOverState.sum
   increments - decrements
  }

  override def getCurrentState = new PNCounterPayload(incVerVec, decVerVec)

  override def merge(replicaPayload: PNCounterPayload): PNCounterPayload = {
    (0 until replicaCount).foreach { idx =>
      incVerVec.modifyAtIndex(idx, Math.max(incVerVec.atIndex(idx), replicaPayload.incVerVec.atIndex(idx)))
      decVerVec.modifyAtIndex(idx, Math.max(decVerVec.atIndex(idx), replicaPayload.decVerVec.atIndex(idx)))
    }
    new PNCounterPayload(incVerVec, decVerVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "pn"

  override def initialValue: PNCounterPayload = new PNCounterPayload(IntVersionVector(replicaCount), IntVersionVector(replicaCount))
}

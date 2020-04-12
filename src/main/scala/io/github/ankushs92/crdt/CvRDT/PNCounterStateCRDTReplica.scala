package io.github.ankushs92.crdt.CvRDT

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.CvRDT.payload.{IntVersionVector, PNCounterPayload}
import io.github.ankushs92.crdt.StateCRDTReplica

case class PNCounterStateCRDTReplica (replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Int, PNCounterPayload, Int] {

  private val incVerVec = IntVersionVector(replicaCount)
  private val decVerVec = IntVersionVector(replicaCount)

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

  override def merge(payload1: PNCounterPayload, payload2: PNCounterPayload): PNCounterPayload = {
    (0 until replicaCount).foreach { idx =>
      incVerVec.modifyAtIndex(idx, Math.max(payload1.incVerVec.atIndex(idx), payload2.incVerVec.atIndex(idx)))
      decVerVec.modifyAtIndex(idx, Math.max(payload1.decVerVec.atIndex(idx), payload2.decVerVec.atIndex(idx)))
    }
    new PNCounterPayload(incVerVec, decVerVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "pn"

}

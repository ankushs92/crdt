package io.github.ankushs92.crdt.StateBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.StateCRDTReplica
import io.github.ankushs92.crdt.payload.{DoubleVersionVector, SummingPayload}

case class SummingStateCRDTReplica(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Double, SummingPayload, Double] {

  private val positiveVerVec = initialValue.positiveVerVec
  private val negVerVec = initialValue.negVerVec

  override def update(write: Double): Unit = {
    if(write < 0) {
      negVerVec.incrementBy(replicaId - 1,  - write)
    }
    else {
      positiveVerVec.incrementBy(replicaId - 1, write)
    }
  }
  override def query: Double = {
    val positive = positiveVerVec.viewOverState.sum
    val negative = negVerVec.viewOverState.sum
    positive - negative
  }

  override def getCurrentState = new SummingPayload(positiveVerVec, negVerVec)

  override def merge(replicaPayload: SummingPayload): SummingPayload = {
    (0 until replicaCount).foreach { idx =>
      positiveVerVec.modifyAtIndex(idx, Math.max(positiveVerVec.atIndex(idx), replicaPayload.positiveVerVec.atIndex(idx)))
      negVerVec.modifyAtIndex(idx, Math.max(negVerVec.atIndex(idx), replicaPayload.negVerVec.atIndex(idx)))
    }
    new SummingPayload(positiveVerVec, negVerVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "sum"

  override def initialValue: SummingPayload = new SummingPayload(DoubleVersionVector(replicaCount), DoubleVersionVector(replicaCount))
}


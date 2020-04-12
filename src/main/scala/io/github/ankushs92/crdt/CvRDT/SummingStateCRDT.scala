package io.github.ankushs92.crdt.CvRDT

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.CvRDT.payload.{DoubleVersionVector, SummingPayload}
import io.github.ankushs92.crdt.StateCRDTReplica

case class SummingStateCRDT(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Double, SummingPayload, Double] {

  private val positiveVerVec = DoubleVersionVector(replicaCount) //Version vector for positive real numbers
  private val negVerVec = DoubleVersionVector(replicaCount) //Version vector for negative real numbers

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

  override def merge(payload1: SummingPayload, payload2: SummingPayload): SummingPayload = {
    (0 until replicaCount).foreach { idx =>
      positiveVerVec.modifyAtIndex(idx, Math.max(payload1.positiveVerVec.atIndex(idx), payload2.positiveVerVec.atIndex(idx)))
      negVerVec.modifyAtIndex(idx, Math.max(payload1.negVerVec.atIndex(idx), payload2.negVerVec.atIndex(idx)))
    }
    new SummingPayload(positiveVerVec, negVerVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "sum"

}


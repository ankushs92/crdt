package io.github.ankushs92.crdt.StateBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.StateCRDTReplica
import io.github.ankushs92.crdt.payload.{AveragingPayload, DoubleVersionVector, IntVersionVector}



class AveragingStateCRDTReplica(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Double, AveragingPayload, Double] {

  private val positiveVerVec = initialValue.positiveVerVec
  private val negVerVec = initialValue.negVerVec
  private val countVec = initialValue.countVec

  override def update(write: Double): Unit = {
    if(write < 0) {
      negVerVec.incrementBy(replicaId -1, - write)
    }
    else {
      positiveVerVec.incrementBy(replicaId -1, write)
    }
    countVec.incrementBy(replicaId, 1)
  }

  override def query: Double = {
    val positive = positiveVerVec.viewOverState.sum
    val negative = negVerVec.viewOverState.sum
    val count = countVec.viewOverState.sum
    (positive - negative) / count
  }

  override def getCurrentState = new AveragingPayload(positiveVerVec, negVerVec, countVec)

  override def merge(replicaPayload: AveragingPayload): AveragingPayload = {
    (0 until replicaCount).foreach { idx =>
      positiveVerVec.modifyAtIndex(idx, Math.max(positiveVerVec.atIndex(idx), replicaPayload.positiveVerVec.atIndex(idx)))
      negVerVec.modifyAtIndex(idx, Math.max(negVerVec.atIndex(idx), replicaPayload.negVerVec.atIndex(idx)))
      countVec.modifyAtIndex(idx, Math.max(countVec.atIndex(idx), replicaPayload.countVec.atIndex(idx)))
    }
    new AveragingPayload(positiveVerVec, negVerVec, countVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "avg"

  override def initialValue: AveragingPayload = new AveragingPayload(DoubleVersionVector(replicaCount) , DoubleVersionVector(replicaCount) , IntVersionVector(replicaCount))
}



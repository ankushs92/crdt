package io.ankushs92.crdt.CvRDT

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.CvRDT.payload.{AveragingPayload, DoubleVersionVector, IntVersionVector}
import io.github.ankushs92.crdt.StateCRDTReplica



class AveragingStateCRDTReplica(replicaId : Int, replicaCount : Int, addr : InetSocketAddress) extends StateCRDTReplica[Double, AveragingPayload, Double] {

  private val positiveVerVec = DoubleVersionVector(replicaCount) //Version vector for positive real numbers
  private val negVerVec = DoubleVersionVector(replicaCount) //Version vector for negative real numbers
  private val countVec = IntVersionVector(replicaCount) //Version vector to maintain counts

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

  override def merge(payload1: AveragingPayload, payload2: AveragingPayload): AveragingPayload = {
    (0 until replicaCount).foreach { idx =>
      positiveVerVec.modifyAtIndex(idx, Math.max(payload1.positiveVerVec.atIndex(idx), payload2.positiveVerVec.atIndex(idx)))
      negVerVec.modifyAtIndex(idx, Math.max(payload1.negVerVec.atIndex(idx), payload2.negVerVec.atIndex(idx)))
      countVec.modifyAtIndex(idx, Math.max(payload1.countVec.atIndex(idx), payload2.countVec.atIndex(idx)))
    }
    new AveragingPayload(positiveVerVec, negVerVec, countVec)
  }

  override def getAddr: InetSocketAddress = addr

  override def getName: String = "avg"
}



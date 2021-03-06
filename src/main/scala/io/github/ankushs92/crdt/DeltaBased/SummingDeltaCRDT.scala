package io.github.ankushs92.crdt.DeltaBased

import java.net.InetSocketAddress

import io.github.ankushs92.crdt.DeltaBased.buffers.{NegativeValueAdd, PositiveValueAdd, SummingDeltaBuffer, SummingDeltaGroup}
import io.github.ankushs92.crdt.payload.{DoubleVersionVector, SummingPayload, SummingState}
import io.github.ankushs92.crdt.{CRDTTypes, DeltaCRDTReplica, Neighbour}


case class SummingDeltaCRDT(
                             id : Int,
                             syncInterval : Int,
                             addr : InetSocketAddress,
                             neighbours : List[Neighbour]
                           ) extends DeltaCRDTReplica[Double, SummingDeltaGroup ,SummingPayload, SummingDeltaBuffer, SummingState, Double] {


  private val posVerVec = bottom.positiveVerVec
  private val negVerVec = bottom.negVerVec

  private val deltaBuffer = new SummingDeltaBuffer(id)
  override def getDeltaBuffer: SummingDeltaBuffer =  deltaBuffer

  override def getSyncInterval: Int = syncInterval

  override def merge(delta: SummingPayload): Unit = synchronized {
    val nbrReplicaId = delta.nbrReplicaId
    val deltaGroups = delta.payload
    deltaGroups.foreach { deltaGroup =>
      val deltaGroupReplicaId = deltaGroup.replicaId
      val idx = deltaGroupReplicaId - 1
      val recPos = deltaGroup.pos
      val recNeg = deltaGroup.neg
      val posPresent = deltaGroup.posPresent
      val negPresent = deltaGroup.negPresent
      val currPos = posVerVec.atIndex(idx)
      val currNeg = negVerVec.atIndex(idx)

      if(posPresent && recPos != currPos) {
        val newCurrPos = Math.max(recPos, currPos)
        posVerVec.modifyAtIndex(idx, newCurrPos)
        deltaBuffer.add(nbrReplicaId, WriteOp.REMOTE, PositiveValueAdd(deltaGroupReplicaId, newCurrPos))
      }

      if(negPresent && recNeg != currNeg) {
        val newCurrNeg = Math.max(recNeg, currNeg)
        negVerVec.modifyAtIndex(idx, newCurrNeg)
        deltaBuffer.add(nbrReplicaId, WriteOp.REMOTE, NegativeValueAdd(deltaGroupReplicaId, newCurrNeg))
      }
    }
  }

  override def getReplicaId: Int = id

  override def bottom: SummingState = synchronized { SummingState(DoubleVersionVector(totalReplicas), DoubleVersionVector(totalReplicas)) }

  override def getName: String = CRDTTypes.SUM

  override def getAddr: InetSocketAddress = addr

  override def update(write: Double): Unit = synchronized {
    val idx = id - 1
    if(isPos(write)) {
      val oldPos = posVerVec.atIndex(idx)
      posVerVec.modifyAtIndex(idx, oldPos + write)
      deltaBuffer.add(id, WriteOp.LOCAL, PositiveValueAdd(id, write))
    }
    else {
      val oldNeg = negVerVec.atIndex(idx)
      val newNeg = -write
      negVerVec.modifyAtIndex(idx, oldNeg + newNeg)
      deltaBuffer.add(id, WriteOp.LOCAL, NegativeValueAdd(id, write))
    }
  }

  override def query: Double = synchronized {
    val totalPos = posVerVec.viewOverState.sum
    val totalNeg = negVerVec.viewOverState.sum
    totalPos - totalNeg
  }

  override def getCurrentState: SummingState = synchronized { SummingState(posVerVec, negVerVec) }

  override def getNeighbours: List[Neighbour] = neighbours

  private def isPos(write : Double) = write >= 0
}
